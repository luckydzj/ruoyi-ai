package org.ruoyi.service.coding.harness.recovery;

import lombok.extern.slf4j.Slf4j;
import org.ruoyi.service.coding.harness.model.HarnessMessage;
import org.ruoyi.service.coding.harness.model.HarnessEvent;
import org.ruoyi.service.coding.harness.model.HarnessMessageRole;
import org.ruoyi.service.coding.harness.model.HarnessInputKind;
import org.ruoyi.service.coding.harness.model.HarnessModelEffect;
import org.ruoyi.service.coding.harness.model.HarnessModelEffectStatus;
import org.ruoyi.service.coding.harness.model.HarnessRunState;
import org.ruoyi.service.coding.harness.model.HarnessRunStatus;
import org.ruoyi.service.coding.harness.model.HarnessSessionState;
import org.ruoyi.service.coding.harness.model.HarnessToolEffect;
import org.ruoyi.service.coding.harness.model.HarnessToolEffectStatus;
import org.ruoyi.service.coding.harness.loop.HarnessTranscriptReader;
import org.ruoyi.service.coding.harness.loop.protocol.HarnessToolBatchCloser;
import org.ruoyi.service.coding.harness.loop.protocol.SyntheticToolResultReason;
import org.ruoyi.service.coding.harness.runtime.HarnessRunRequest;
import org.ruoyi.service.coding.harness.runtime.HarnessScheduleResult;
import org.ruoyi.service.coding.harness.runtime.HarnessScheduler;
import org.ruoyi.service.coding.harness.store.HarnessOptimisticLockException;
import org.ruoyi.service.coding.harness.store.HarnessRunScanPage;
import org.ruoyi.service.coding.harness.store.HarnessStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Re-enqueues durable work after a single-process restart. Waiting runs remain waiting. A model
 * request whose outcome is not provably present in the immutable message ledger is quarantined,
 * never blindly replayed.
 */
@Slf4j
@Component
public class HarnessStartupRecovery {

    private static final int MESSAGE_PAGE_SIZE = 256;

    private final HarnessStore store;
    private final HarnessScheduler scheduler;
    private final boolean enabled;
    private final int pageSize;
    private final int maxRuns;
    private final int maxScanRecords;
    private final int maxMessagesPerEffect;
    private final HarnessToolBatchCloser toolBatchCloser;
    private final AtomicReference<RecoveryLifecycle> lifecycle =
        new AtomicReference<>(RecoveryLifecycle.NEW);
    private volatile HarnessRecoveryReport lastReport;

    @Autowired
    public HarnessStartupRecovery(
        HarnessStore store,
        HarnessScheduler scheduler,
        @Value("${coding.harness.recovery.enabled:true}") boolean enabled,
        @Value("${coding.harness.recovery.page-size:64}") int pageSize,
        @Value("${coding.harness.recovery.max-runs:200}") int maxRuns,
        @Value("${coding.harness.recovery.max-scan-records:100000}") int maxScanRecords,
        @Value("${coding.harness.recovery.max-messages-per-effect:10000}")
        int maxMessagesPerEffect
    ) {
        if (pageSize < 1 || pageSize > 1_000 || maxRuns < 1 || maxScanRecords < maxRuns
            || maxMessagesPerEffect < 1) {
            throw new IllegalArgumentException("Invalid Harness startup recovery limits");
        }
        this.store = store;
        this.scheduler = scheduler;
        this.enabled = enabled;
        this.pageSize = pageSize;
        this.maxRuns = maxRuns;
        this.maxScanRecords = maxScanRecords;
        this.maxMessagesPerEffect = maxMessagesPerEffect;
        this.toolBatchCloser = new HarnessToolBatchCloser(store,
            new HarnessTranscriptReader(store));
    }

    public HarnessStartupRecovery(HarnessStore store, HarnessScheduler scheduler,
                                  boolean enabled, int pageSize, int maxRuns,
                                  int maxMessagesPerEffect) {
        this(store, scheduler, enabled, pageSize, maxRuns,
            Math.max(100_000, maxRuns), maxMessagesPerEffect);
    }

    @EventListener(ApplicationReadyEvent.class)
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public void onApplicationReady() {
        HarnessRecoveryReport report = recover();
        if (!report.idempotentNoop()) {
            log.info("Harness startup recovery scanned={}, scheduled={}, duplicate={}, waiting={}, "
                    + "terminal={}, quarantined={}, truncated={}",
                report.scanned(), report.scheduled(), report.alreadyScheduled(),
                report.waitingSkipped(), report.terminalSkipped(), report.quarantined(),
                report.truncated());
        }
    }

    /** Safe to call more than once; only the first successful invocation performs a scan. */
    public HarnessRecoveryReport recover() {
        if (!enabled) {
            return HarnessRecoveryReport.disabledOrAlreadyRun();
        }
        if (!lifecycle.compareAndSet(RecoveryLifecycle.NEW, RecoveryLifecycle.RUNNING)) {
            HarnessRecoveryReport completed = lastReport;
            return completed == null ? HarnessRecoveryReport.disabledOrAlreadyRun()
                : new HarnessRecoveryReport(completed.scanned(), completed.scheduled(),
                    completed.alreadyScheduled(), completed.waitingSkipped(),
                    completed.terminalSkipped(), completed.quarantined(), completed.truncated(), true);
        }
        try {
            HarnessRecoveryReport report = doRecover();
            lastReport = report;
            lifecycle.set(RecoveryLifecycle.COMPLETED);
            return report;
        } catch (RuntimeException failure) {
            // A retry is safe: the keyed scheduler deduplicates requests already accepted before
            // the failure, while the scan cursor starts again from immutable durable snapshots.
            lifecycle.set(RecoveryLifecycle.NEW);
            throw failure;
        }
    }

    private HarnessRecoveryReport doRecover() {
        MutableReport report = new MutableReport();
        String cursor = null;
        while (report.actionable < maxRuns && report.scanned < maxScanRecords) {
            int remaining = maxScanRecords - report.scanned;
            HarnessRunScanPage page = store.scanRunsForRecovery(cursor,
                Math.min(pageSize, remaining));
            if (page.runs().isEmpty()) {
                report.truncated = page.hasMore();
                break;
            }
            for (HarnessRunState discovered : page.runs()) {
                report.scanned++;
                if (discovered.status() == HarnessRunStatus.QUEUED
                    || discovered.status() == HarnessRunStatus.RUNNING) {
                    report.actionable++;
                }
                recoverRun(discovered, report);
                if (report.actionable >= maxRuns) {
                    break;
                }
            }
            if (!page.hasMore()) {
                break;
            }
            cursor = page.nextCursor();
            if (report.actionable >= maxRuns || report.scanned >= maxScanRecords) {
                report.truncated = true;
            }
        }
        return report.freeze();
    }

    private void recoverRun(HarnessRunState discovered, MutableReport report) {
        discovered = reconcileControlEventOutbox(discovered);
        HarnessRunStatus status = discovered.status();
        if (status.isTerminal()) {
            repairTerminalEvent(discovered);
            report.terminalSkipped++;
            return;
        }
        // Cancellation is a durable user decision, not a best-effort in-process signal. Honor it
        // before inspecting uncertain provider effects so a restart cannot downgrade an accepted
        // cancel into a permanently suspended run.
        if (discovered.cancellationRequested()) {
            honorCancellation(discovered);
            report.terminalSkipped++;
            return;
        }
        if (status != HarnessRunStatus.QUEUED && status != HarnessRunStatus.RUNNING) {
            report.waitingSkipped++;
            return;
        }

        Optional<HarnessRunState> creationRepaired;
        try {
            creationRepaired = repairCreationState(discovered);
        } catch (RuntimeException repairFailure) {
            log.warn("Unable to repair creation stages for Harness run {}",
                discovered.runId(), repairFailure);
            quarantine(discovered, "Creation recovery failed: "
                + safeMessage(repairFailure), 0);
            report.quarantined++;
            return;
        }
        if (creationRepaired.isEmpty()) {
            report.quarantined++;
            return;
        }
        Optional<HarnessRunState> safe = makeModelEffectSafe(creationRepaired.get());
        if (safe.isEmpty()) {
            report.quarantined++;
            return;
        }
        HarnessRunState run = safe.get();
        // A concurrent request may have changed state while an effect ledger was inspected.
        if (run.status() != HarnessRunStatus.QUEUED
            && run.status() != HarnessRunStatus.RUNNING) {
            report.waitingSkipped++;
            return;
        }
        HarnessScheduleResult result = scheduler.schedule(new HarnessRunRequest(
            run.owner(), run.sessionId(), run.runId()));
        if (result == HarnessScheduleResult.SCHEDULED) {
            report.scheduled++;
        } else {
            report.alreadyScheduled++;
        }
    }

    /**
     * Drains committed plan-event drafts for every status, including waiting and terminal runs
     * which will not be scheduled again. A crash after append but before the snapshot marker is
     * repaired by matching the immutable event id, never by appending a second event.
     */
    private HarnessRunState reconcileControlEventOutbox(HarnessRunState initial) {
        HarnessRunState run = initial;
        for (int attempt = 0; attempt < 4; attempt++) {
            HarnessRunState next = run;
            for (HarnessToolEffect observed : run.toolEffects().values()) {
                HarnessToolEffect effect = next.toolEffects().get(observed.toolCallId());
                if (effect == null || !effect.hasPendingControlEvent()) {
                    continue;
                }
                HarnessEvent event = effect.controlEvent();
                if (!controlEventExists(run, event)) {
                    store.appendEvent(run.owner(), event);
                }
                next = next.withToolEffect(effect.markControlEventPublished(), now());
            }
            if (next == run) {
                return run;
            }
            try {
                return store.saveRun(run.owner(), next, run.revision());
            } catch (HarnessOptimisticLockException conflict) {
                run = reload(run);
            }
        }
        throw new IllegalStateException("Unable to acknowledge control-event outbox for run "
            + initial.runId());
    }

    private boolean controlEventExists(HarnessRunState run, HarnessEvent expected) {
        long cursor = 0;
        int inspected = 0;
        while (inspected < maxScanRecords) {
            int limit = Math.min(MESSAGE_PAGE_SIZE, maxScanRecords - inspected);
            List<HarnessEvent> page = store.readEvents(run.owner(), run.sessionId(), run.runId(),
                cursor, limit);
            if (page.isEmpty()) {
                return false;
            }
            for (HarnessEvent event : page) {
                if (event.sequence() <= cursor) {
                    throw new IllegalStateException(
                        "Harness event scan did not advance during outbox recovery");
                }
                cursor = event.sequence();
                inspected++;
                if (expected.eventId().equals(event.eventId())) {
                    if (!expected.withSequence(event.sequence()).equals(event)) {
                        throw new IllegalStateException(
                            "Control event id was reused for different event content");
                    }
                    return true;
                }
            }
            if (page.size() < limit) {
                return false;
            }
        }
        if (store.readEvents(run.owner(), run.sessionId(), run.runId(), cursor, 1).isEmpty()) {
            return false;
        }
        throw new IllegalStateException("Control event recovery scan exceeded "
            + maxScanRecords + " records");
    }

    /**
     * Run creation spans session pointer, run snapshot and initial USER ledger writes. A crash can
     * leave the QUEUED snapshot as the only durable stage. Repair the other two stages before the
     * scheduler is allowed to expose the run to the model.
     */
    private Optional<HarnessRunState> repairCreationState(HarnessRunState run) {
        HarnessSessionState session = store.findSession(run.owner(), run.sessionId()).orElse(null);
        if (session == null) {
            return quarantine(run, "Run has no durable owning session", 0);
        }
        for (int attempt = 0; attempt < 4 && !run.runId().equals(session.activeRunId()); attempt++) {
            if (session.activeRunId() != null) {
                HarnessRunState active = store.findRun(run.owner(), run.sessionId(),
                    session.activeRunId()).orElse(null);
                if (active != null && !active.status().isTerminal()) {
                    return quarantine(run,
                        "Session points to a different non-terminal run during recovery", 0);
                }
            }
            try {
                session = store.saveSession(run.owner(),
                    session.withActiveRun(run.runId(), now()), session.revision());
            } catch (HarnessOptimisticLockException conflict) {
                session = store.findSession(run.owner(), run.sessionId())
                    .orElseThrow(() -> new IllegalStateException(
                        "Harness session disappeared during startup recovery"));
            }
        }
        if (!run.runId().equals(session.activeRunId())) {
            return quarantine(run, "Unable to repair the active run pointer", 0);
        }

        String inputId = "run-create:" + run.runId();
        if (!messageWithInputIdExists(run, inputId)) {
            store.appendMessage(run.owner(), HarnessMessage.draft(run.sessionId(), run.runId(),
                HarnessMessageRole.USER, run.originalRequirement(), null, List.of(), null, null,
                false, null, Map.of("kind", HarnessInputKind.INITIAL.name(),
                    "inputId", inputId), now()));
        }
        return Optional.of(run);
    }

    private void repairTerminalEvent(HarnessRunState run) {
        String expectedType = switch (run.status()) {
            case COMPLETED -> "run.completed";
            case FAILED -> "run.failed";
            case CANCELLED -> "run.cancelled";
            default -> throw new IllegalArgumentException("Run is not terminal");
        };
        long cursor = 0;
        while (true) {
            List<HarnessEvent> page = store.readEvents(run.owner(), run.sessionId(), run.runId(),
                cursor, MESSAGE_PAGE_SIZE);
            if (page.stream().anyMatch(event -> expectedType.equals(event.type()))) {
                return;
            }
            if (page.isEmpty() || page.size() < MESSAGE_PAGE_SIZE) {
                Map<String, Object> data = new java.util.LinkedHashMap<>();
                data.put("status", run.status().name());
                data.put("revision", run.revision());
                if (run.error() != null && !run.error().isBlank()) {
                    data.put(run.status() == HarnessRunStatus.FAILED ? "message" : "reason",
                        run.error());
                }
                store.appendEvent(run.owner(), HarnessEvent.draft(run.sessionId(), run.runId(),
                    expectedType, null, null, null, data, now()));
                return;
            }
            long next = page.get(page.size() - 1).sequence();
            if (next <= cursor) {
                throw new IllegalStateException("Harness event scan did not advance its cursor");
            }
            cursor = next;
        }
    }

    private boolean messageWithInputIdExists(HarnessRunState run, String inputId) {
        long cursor = 0;
        while (true) {
            List<HarnessMessage> page = store.readMessages(run.owner(), run.sessionId(), cursor,
                MESSAGE_PAGE_SIZE);
            if (page.isEmpty()) {
                return false;
            }
            for (HarnessMessage message : page) {
                if (message.sequence() <= cursor) {
                    throw new IllegalStateException(
                        "Harness message scan did not advance its cursor");
                }
                cursor = message.sequence();
                if (inputId.equals(message.metadata().get("inputId"))) {
                    return true;
                }
            }
            if (page.size() < MESSAGE_PAGE_SIZE) {
                return false;
            }
        }
    }

    private String safeMessage(Throwable failure) {
        String message = failure.getMessage();
        return message == null || message.isBlank()
            ? failure.getClass().getSimpleName() : message;
    }

    private Optional<HarnessRunState> makeModelEffectSafe(HarnessRunState initial) {
        HarnessRunState run = initial;
        for (int attempt = 0; attempt < 4; attempt++) {
            HarnessModelEffect effect = run.modelEffect();
            if (effect == null) {
                return Optional.of(run);
            }
            if (effect.iteration() != run.iteration()) {
                return quarantine(run, "Model effect iteration does not match the durable run", attempt);
            }

            if (effect.status() == HarnessModelEffectStatus.SETTLED) {
                // The aggregate constructor guarantees a settled effect has a response message id.
                // Its raw message may be older than the current compaction checkpoint, so do not
                // mistake an intentionally compacted response for an unknown provider outcome.
                return Optional.of(run);
            }
            if (effect.status() == HarnessModelEffectStatus.ABANDONED) {
                return quarantine(run, "Abandoned model effect cannot be resumed automatically", attempt);
            }
            Optional<HarnessMessage> response = findPersistedResponse(run, effect);
            if (response.isEmpty()) {
                return quarantine(run,
                    "Process restarted with an unsettled provider request", attempt);
            }

            long settledAt = now();
            HarnessRunState next = run.withModelEffect(
                effect.settle(response.get().messageId(), settledAt), settledAt);
            // Native snapshots account usage in the same durable revision that settles the
            // provider effect. Legacy snapshots deliberately leave this to the processor's
            // one-time ledger fold, otherwise the recovered response would be counted twice.
            if (run.usageInitialized()) {
                next = next.addModelUsage(response.get().usage(), settledAt);
            }
            try {
                return Optional.of(store.saveRun(run.owner(), next, run.revision()));
            } catch (HarnessOptimisticLockException conflict) {
                run = reload(run);
                if (!isRecoverable(run)) {
                    return Optional.of(run);
                }
            }
        }
        return Optional.empty();
    }

    private HarnessRunState honorCancellation(HarnessRunState initial) {
        HarnessRunState run = initial;
        for (int attempt = 0; attempt < 4; attempt++) {
            if (run.status().isTerminal()) {
                return run;
            }
            long timestamp = now();
            HarnessToolBatchCloser.Closure closure = toolBatchCloser.close(run,
                SyntheticToolResultReason.CANCEL, timestamp);
            HarnessRunState next = closure.run();
            for (HarnessToolEffect effect : next.toolEffects().values()) {
                if (effect.status() == HarnessToolEffectStatus.PENDING) {
                    next = next.withToolEffect(
                        effect.abandon("Cancellation recovered after restart", timestamp),
                        timestamp);
                }
            }
            HarnessModelEffect modelEffect = next.modelEffect();
            if (modelEffect != null && modelEffect.status() == HarnessModelEffectStatus.PENDING) {
                next = next.withModelEffect(
                    modelEffect.abandon("Cancellation recovered after restart", timestamp),
                    timestamp);
            }
            next = next.transition(HarnessRunStatus.CANCELLED, null, timestamp);
            try {
                return store.saveRun(run.owner(), next, next.revision());
            } catch (HarnessOptimisticLockException conflict) {
                run = reload(run);
            }
        }
        throw new IllegalStateException(
            "Unable to persist durable cancellation for run " + initial.runId());
    }

    private Optional<HarnessRunState> quarantine(HarnessRunState run, String reason, int attempt) {
        HarnessRunState next = run;
        HarnessModelEffect effect = run.modelEffect();
        if (effect != null && effect.status() == HarnessModelEffectStatus.PENDING) {
            next = next.withModelEffect(effect.abandon(reason, now()), now());
        }
        HarnessRunStatus target = run.status() == HarnessRunStatus.RUNNING
            ? HarnessRunStatus.SUSPENDED : HarnessRunStatus.FAILED;
        next = next.transition(target, reason, now());
        try {
            store.saveRun(run.owner(), next, run.revision());
            log.warn("Quarantined Harness run {} during startup recovery: {}", run.runId(), reason);
            return Optional.empty();
        } catch (HarnessOptimisticLockException conflict) {
            if (attempt >= 3) {
                return Optional.empty();
            }
            HarnessRunState current = reload(run);
            if (!isRecoverable(current)) {
                return Optional.of(current);
            }
            return makeModelEffectSafe(current);
        }
    }

    private Optional<HarnessMessage> findPersistedResponse(HarnessRunState run,
                                                           HarnessModelEffect effect) {
        long cursor = run.contextCheckpoint().compactedThroughMessageSequence();
        int inspected = 0;
        while (inspected < maxMessagesPerEffect) {
            int limit = Math.min(MESSAGE_PAGE_SIZE, maxMessagesPerEffect - inspected);
            List<HarnessMessage> page = store.readMessages(run.owner(), run.sessionId(), cursor, limit);
            if (page.isEmpty()) {
                return Optional.empty();
            }
            for (HarnessMessage message : page) {
                if (message.sequence() <= cursor) {
                    throw new IllegalStateException("Harness message scan did not advance its cursor");
                }
                cursor = message.sequence();
                inspected++;
                if (message.role() == HarnessMessageRole.ASSISTANT
                    && run.runId().equals(message.runId())
                    && effect.effectId().equals(String.valueOf(message.metadata().get("effectId")))) {
                    return Optional.of(message);
                }
            }
            if (page.size() < limit) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    private HarnessRunState reload(HarnessRunState run) {
        return store.findRun(run.owner(), run.sessionId(), run.runId())
            .orElseThrow(() -> new IllegalStateException(
                "Harness run disappeared during startup recovery: " + run.runId()));
    }

    private boolean isRecoverable(HarnessRunState run) {
        return run.status() == HarnessRunStatus.QUEUED
            || run.status() == HarnessRunStatus.RUNNING;
    }

    private long now() {
        return System.currentTimeMillis();
    }

    private enum RecoveryLifecycle {
        NEW,
        RUNNING,
        COMPLETED
    }

    private static final class MutableReport {
        private int scanned;
        private int actionable;
        private int scheduled;
        private int alreadyScheduled;
        private int waitingSkipped;
        private int terminalSkipped;
        private int quarantined;
        private boolean truncated;

        private HarnessRecoveryReport freeze() {
            return new HarnessRecoveryReport(scanned, scheduled, alreadyScheduled,
                waitingSkipped, terminalSkipped, quarantined, truncated, false);
        }
    }
}
