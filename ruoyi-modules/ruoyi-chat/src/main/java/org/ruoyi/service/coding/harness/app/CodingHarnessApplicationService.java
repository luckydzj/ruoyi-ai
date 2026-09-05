package org.ruoyi.service.coding.harness.app;

import lombok.extern.slf4j.Slf4j;
import org.ruoyi.service.coding.CodingWorkspaceService;
import org.ruoyi.service.coding.harness.approval.ApprovalDecision;
import org.ruoyi.service.coding.harness.approval.ApprovalState;
import org.ruoyi.service.coding.harness.approval.ResolveApprovalCommand;
import org.ruoyi.service.coding.harness.approval.ToolCallApprovalAggregate;
import org.ruoyi.service.coding.harness.context.CompactionControl;
import org.ruoyi.service.coding.harness.event.HarnessEventHub;
import org.ruoyi.service.coding.harness.event.HarnessEventOutboxService;
import org.ruoyi.service.coding.harness.loop.HarnessTranscriptReader;
import org.ruoyi.service.coding.harness.loop.protocol.HarnessToolBatchCloser;
import org.ruoyi.service.coding.harness.loop.protocol.SyntheticToolResultReason;
import org.ruoyi.service.coding.harness.loop.protocol.ToolProtocolValidation;
import org.ruoyi.service.coding.harness.loop.protocol.ToolProtocolValidator;
import org.ruoyi.service.coding.harness.model.HarnessEvent;
import org.ruoyi.service.coding.harness.model.HarnessApproval;
import org.ruoyi.service.coding.harness.model.HarnessApprovalPolicy;
import org.ruoyi.service.coding.harness.model.HarnessApprovalStatus;
import org.ruoyi.service.coding.harness.model.HarnessBudget;
import org.ruoyi.service.coding.harness.model.HarnessInputKind;
import org.ruoyi.service.coding.harness.model.HarnessMessage;
import org.ruoyi.service.coding.harness.model.HarnessMessageRole;
import org.ruoyi.service.coding.harness.model.HarnessOwner;
import org.ruoyi.service.coding.harness.model.HarnessPermissionMode;
import org.ruoyi.service.coding.harness.model.HarnessQueuedInput;
import org.ruoyi.service.coding.harness.model.HarnessRunState;
import org.ruoyi.service.coding.harness.model.HarnessRunStatus;
import org.ruoyi.service.coding.harness.model.HarnessSessionState;
import org.ruoyi.service.coding.harness.model.HarnessToolEffect;
import org.ruoyi.service.coding.harness.model.HarnessToolEffectStatus;
import org.ruoyi.service.coding.harness.plan.PlanAggregate;
import org.ruoyi.service.coding.harness.plan.PlanApprovalCommand;
import org.ruoyi.service.coding.harness.plan.ExecutionMode;
import org.ruoyi.service.coding.harness.runtime.HarnessRunRequest;
import org.ruoyi.service.coding.harness.runtime.HarnessActiveTurnRegistry;
import org.ruoyi.service.coding.harness.runtime.HarnessScheduler;
import org.ruoyi.service.coding.harness.runtime.HarnessSessionGate;
import org.ruoyi.service.coding.harness.store.HarnessOptimisticLockException;
import org.ruoyi.service.coding.harness.store.HarnessStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Authenticated command/query facade. It never derives ownership from client identifiers. */
@Service
@Slf4j
public class CodingHarnessApplicationService {

    static final int MAX_PENDING_INPUTS_PER_RUN = 64;
    static final int MAX_PENDING_INPUT_BYTES_PER_RUN = 1_048_576;
    private static final String INCOMPLETE_PLAN_SUSPENSION =
        "Model stopped before the authoritative plan passed verification";
    private final HarnessStore store;
    private final HarnessEventHub eventHub;
    private final HarnessEventOutboxService eventOutboxService;
    private final HarnessScheduler scheduler;
    private final HarnessSessionGate sessionGate;
    private final CodingWorkspaceService workspaceService;
    private final HarnessActiveTurnRegistry activeTurns;
    private final HarnessTranscriptReader transcriptReader;
    private final ToolProtocolValidator toolProtocolValidator = new ToolProtocolValidator();
    private final HarnessToolBatchCloser toolBatchCloser;
    private final HarnessBudgetPolicy budgetPolicy;

    @Autowired
    public CodingHarnessApplicationService(HarnessStore store, HarnessEventHub eventHub,
                                           HarnessEventOutboxService eventOutboxService,
                                           HarnessScheduler scheduler, HarnessSessionGate sessionGate,
                                           CodingWorkspaceService workspaceService,
                                           HarnessActiveTurnRegistry activeTurns,
                                           HarnessBudgetPolicy budgetPolicy) {
        this.store = store;
        this.eventHub = eventHub;
        this.eventOutboxService = Objects.requireNonNull(eventOutboxService,
            "eventOutboxService");
        this.scheduler = scheduler;
        this.sessionGate = sessionGate;
        this.workspaceService = workspaceService;
        this.activeTurns = activeTurns;
        this.transcriptReader = new HarnessTranscriptReader(store);
        this.toolBatchCloser = new HarnessToolBatchCloser(store, transcriptReader);
        this.budgetPolicy = Objects.requireNonNull(budgetPolicy, "budgetPolicy");
    }

    public CodingHarnessApplicationService(HarnessStore store, HarnessEventHub eventHub,
                                           HarnessScheduler scheduler, HarnessSessionGate sessionGate,
                                           CodingWorkspaceService workspaceService,
                                           HarnessActiveTurnRegistry activeTurns,
                                           HarnessBudgetPolicy budgetPolicy) {
        this(store, eventHub, new HarnessEventOutboxService(store, eventHub), scheduler,
            sessionGate, workspaceService, activeTurns, budgetPolicy);
    }

    public CodingHarnessApplicationService(HarnessStore store, HarnessEventHub eventHub,
                                           HarnessScheduler scheduler,
                                           HarnessSessionGate sessionGate,
                                           CodingWorkspaceService workspaceService,
                                           HarnessActiveTurnRegistry activeTurns) {
        this(store, eventHub, scheduler, sessionGate, workspaceService, activeTurns,
            HarnessBudgetPolicy.secureDefaults());
    }

    public HarnessSessionState createSession(HarnessOwner owner, CreateHarnessSessionCommand command) {
        if (command == null || command.model() == null || command.model().isBlank()) {
            throw new IllegalArgumentException("model is required");
        }
        Path workspace = canonicalWorkspace(owner, command.workspacePath());
        String model = command.model().strip();
        HarnessPermissionMode permissionMode = command.permissionMode() == null
            ? HarnessPermissionMode.READ_ONLY : command.permissionMode();
        HarnessApprovalPolicy approvalPolicy = command.approvalPolicy() == null
            ? HarnessApprovalPolicy.ON_REQUEST : command.approvalPolicy();
        String sessionId = stableId("session", owner, null, command.idempotencyKey());
        return sessionGate.withSession(owner, sessionId, () -> {
            HarnessSessionState existing = store.findSession(owner, sessionId).orElse(null);
            if (existing != null) {
                if (!existing.workspace().equals(workspace.toString())
                    || !existing.model().equals(model)
                    || existing.permissionMode() != permissionMode
                    || existing.approvalPolicy() != approvalPolicy
                    || !Objects.equals(existing.title(), command.title())) {
                    throw new HarnessConflictException(
                        "idempotencyKey was already used for a different session request");
                }
                return existing;
            }
            long now = System.currentTimeMillis();
            return store.createSession(HarnessSessionState.createWithId(sessionId, owner,
                workspace.toString(), model, permissionMode, approvalPolicy, command.title(), now));
        });
    }

    public List<HarnessSessionState> listSessions(HarnessOwner owner) {
        return store.listSessions(owner);
    }

    public HarnessSessionState getSession(HarnessOwner owner, String sessionId) {
        return store.findSession(owner, sessionId)
            .orElseThrow(() -> new HarnessNotFoundException("session", sessionId));
    }

    public List<HarnessRunState> listRuns(HarnessOwner owner, String sessionId) {
        getSession(owner, sessionId);
        return store.listRuns(owner, sessionId);
    }

    public HarnessRunState getRun(HarnessOwner owner, String sessionId, String runId) {
        return store.findRun(owner, sessionId, runId)
            .orElseThrow(() -> new HarnessNotFoundException("run", runId));
    }

    public HarnessRunState createRun(HarnessOwner owner, String sessionId,
                                     CreateHarnessRunCommand command) {
        if (command == null) {
            throw new IllegalArgumentException("run command is required");
        }
        HarnessBudget enforcedBudget = budgetPolicy.enforce(command.budget());
        return sessionGate.withSession(owner, sessionId, () -> {
            HarnessSessionState session = getSession(owner, sessionId);
            String runId = stableId("run", owner, sessionId, command.idempotencyKey());
            HarnessRunState existing = store.findRun(owner, sessionId, runId).orElse(null);
            if (existing != null) {
                if (!existing.originalRequirement().equals(command.requirement())
                    || !existing.budget().equals(enforcedBudget)) {
                    throw new HarnessConflictException(
                        "idempotencyKey was already used for a different run request");
                }
                return repairIdempotentRunCreation(owner, session, existing);
            }
            requireNoActiveRun(owner, session);
            long now = System.currentTimeMillis();
            HarnessRunState candidate = HarnessRunState.createWithId(runId, session,
                command.requirement(), enforcedBudget, now);
            HarnessRunState predecessor = session.activeRunId() == null ? null
                : store.findRun(owner, sessionId, session.activeRunId()).orElse(null);
            if (predecessor != null && predecessor.status().isTerminal()
                && !predecessor.contextCheckpoint().isEmpty()) {
                // A session sequence is global across follow-up runs. Reusing the predecessor's
                // durable boundary prevents every new run from rematerializing archived history;
                // failure counters are run-local and deliberately reset.
                candidate = candidate.withContextState(predecessor.contextCheckpoint(),
                    CompactionControl.initial(), now);
            }
            HarnessRunRequest request = new HarnessRunRequest(owner, sessionId, runId);
            // Admission precedes every durable mutation for this new run. A capacity exception is
            // therefore an honest HTTP 429: no run, session pointer, message or event was written.
            try (HarnessScheduler.Admission admission = scheduler.reserve(request)) {
                HarnessRunState run = store.createRun(owner, candidate);
                try {
                    session = store.saveSession(owner, session.withActiveRun(run.runId(), now),
                        session.revision());
                } catch (HarnessOptimisticLockException conflict) {
                    HarnessRunState cancelled = run.transition(HarnessRunStatus.CANCELLED,
                        "A concurrent run won the session lease", System.currentTimeMillis());
                    store.saveRun(owner, cancelled, run.revision());
                    throw new HarnessConflictException("Another run was created concurrently", conflict);
                }

                appendInitialMessageIfMissing(owner, run, now);
                admission.commit();
                // The session gate keeps the worker from publishing run.started until run.created
                // is durable, while committing first prevents an event-ledger outage from
                // orphaning work.
                eventHub.publish(owner, HarnessEvent.draft(sessionId, run.runId(), "run.created",
                    null, null, null, Map.of("status", run.status().name()), now));
                return run;
            }
        });
    }

    public HarnessRunState queueInput(HarnessOwner owner, String sessionId, String runId,
                                      QueueHarnessInputCommand command) {
        return sessionGate.withSession(owner, sessionId, () -> {
            HarnessRunState run = getRun(owner, sessionId, runId);
            if (run.status().isTerminal()) {
                if (command.kind() == HarnessInputKind.FOLLOW_UP) {
                    return createRun(owner, sessionId,
                        new CreateHarnessRunCommand(command.content(),
                            budgetPolicy.forFollowUp(run.budget()),
                            "follow-up:" + command.idempotencyKey()));
                }
                throw new HarnessConflictException("Cannot steer a terminal run");
            }
            long now = System.currentTimeMillis();
            String inputId = stableId("input", owner, sessionId,
                runId + "\u0000" + command.idempotencyKey());
            HarnessQueuedInput existingInput = run.pendingInputs().stream()
                .filter(candidate -> candidate.inputId().equals(inputId)).findFirst().orElse(null);
            HarnessMessage existingMessage = findMessageByInputId(owner, sessionId, inputId);
            if ((existingInput != null && (existingInput.kind() != command.kind()
                || !existingInput.content().equals(command.content())))
                || (existingMessage != null && (!command.kind().name().equals(
                    existingMessage.metadata().get("kind"))
                    || !command.content().equals(existingMessage.content())))) {
                throw new HarnessConflictException(
                    "idempotencyKey was already used for a different queued input");
            }
            HarnessRunState next = run;
            boolean queuedInputPresent = existingInput != null;
            HarnessQueuedInput eventInput = existingInput;
            if (existingInput == null && existingMessage == null) {
                requirePendingInputCapacity(run, command.content());
                HarnessQueuedInput input = HarnessQueuedInput.createWithId(inputId,
                    command.kind(), command.content(), now);
                next = next.enqueue(input, now);
                queuedInputPresent = true;
                eventInput = input;
            }
            boolean wake = queuedInputPresent
                && next.status() == HarnessRunStatus.WAITING_FOR_INPUT;
            if (wake) {
                next = next.transition(HarnessRunStatus.QUEUED, null, now);
            }
            if (existingMessage == null) {
                if (eventInput == null) {
                    throw new IllegalStateException(
                        "Queued input event has no durable input identity");
                }
                String eventId = stableId("event", owner, sessionId,
                    runId + "\u0000input.queued\u0000" + inputId);
                next = next.enqueueEvent(HarnessEvent.draftWithId(eventId, sessionId, runId,
                    "input.queued", null, null, null, Map.of("inputId", inputId,
                        "kind", command.kind().name()), eventInput.createdAt()), now);
            }
            HarnessRunState saved = next == run ? run
                : store.saveRun(owner, next, run.revision());
            if (existingMessage == null) {
                store.appendMessage(owner, HarnessMessage.draft(sessionId, runId,
                    HarnessMessageRole.CONTROL, command.content(), null, List.of(), null, null,
                    false, null, Map.of("kind", command.kind().name(), "queued", true,
                        "inputId", inputId), now));
            }
            if (saved.status() == HarnessRunStatus.QUEUED) {
                scheduler.schedule(new HarnessRunRequest(owner, sessionId, runId));
            }
            return eventOutboxService.drainBestEffort(owner, saved);
        });
    }

    public HarnessRunState cancel(HarnessOwner owner, String sessionId, String runId) {
        return sessionGate.withSession(owner, sessionId, () -> {
            HarnessRunState run = getRun(owner, sessionId, runId);
            if (run.status().isTerminal()) {
                return run;
            }
            long now = System.currentTimeMillis();
            HarnessRunRequest request = new HarnessRunRequest(owner, sessionId, runId);
            boolean removedFromQueue = scheduler.cancelQueued(request);
            HarnessRunState next;
            String eventType;
            boolean interruptActive = false;
            if (removedFromQueue || run.status() != HarnessRunStatus.RUNNING) {
                HarnessRunState reconciled = acknowledgePendingControlEvents(owner, run, now);
                HarnessToolBatchCloser.Closure closure = toolBatchCloser.close(reconciled,
                    SyntheticToolResultReason.CANCEL, now);
                next = abandonPendingToolEffects(closure.run(), "Cancellation requested", now)
                    .transition(HarnessRunStatus.CANCELLED, null, now);
                eventType = "run.cancelled";
            } else {
                next = run.requestCancellation(now);
                eventType = "run.cancel.requested";
                interruptActive = true;
            }
            HarnessRunState saved = store.saveRun(owner, next, next.revision());
            // Persist the authoritative decision before signalling the worker. The interrupted
            // lane can now re-read cancellationRequested without racing an earlier snapshot.
            if (interruptActive) {
                activeTurns.cancel(request);
            }
            eventHub.publish(owner, HarnessEvent.draft(sessionId, runId, eventType,
                null, null, null, Map.of("status", saved.status().name()), now));
            return saved;
        });
    }

    /**
     * Best-effort delivery of control events before a waiting/queued run becomes terminal. The
     * exact draft remains attached to the effect if delivery fails, so cancellation still returns
     * the authoritative committed tool result and startup recovery can retry by stable event id.
     */
    private HarnessRunState acknowledgePendingControlEvents(HarnessOwner owner,
                                                             HarnessRunState run, long now) {
        HarnessRunState next = run;
        for (HarnessToolEffect observed : run.toolEffects().values()) {
            HarnessToolEffect effect = next.toolEffects().get(observed.toolCallId());
            if (effect == null || !effect.hasPendingControlEvent()) {
                continue;
            }
            HarnessEvent event = effect.controlEvent();
            try {
                eventHub.publishIdempotent(owner, event);
                next = next.withToolEffect(effect.markControlEventPublished(), now);
            } catch (RuntimeException publicationFailure) {
                log.warn("Deferred control event {} while cancelling run {}",
                    event.eventId(), run.runId(), publicationFailure);
            }
        }
        return next;
    }

    public HarnessRunState resume(HarnessOwner owner, String sessionId, String runId) {
        return sessionGate.withSession(owner, sessionId, () -> {
            HarnessRunState run = getRun(owner, sessionId, runId);
            if (run.status() == HarnessRunStatus.RUNNING) {
                return run;
            }
            if (run.status() != HarnessRunStatus.SUSPENDED
                && run.status() != HarnessRunStatus.QUEUED) {
                throw new HarnessConflictException("Only a suspended run can be resumed");
            }
            long now = System.currentTimeMillis();
            HarnessRunState resumable = enqueueIncompletePlanResumeInput(run, now);
            HarnessRunState queued = run.status() == HarnessRunStatus.QUEUED ? run
                : store.saveRun(owner,
                    resumable.transition(HarnessRunStatus.QUEUED, null, now), run.revision());
            scheduler.schedule(new HarnessRunRequest(owner, sessionId, runId));
            if (queued != run) {
                eventHub.publish(owner, HarnessEvent.draft(sessionId, runId, "run.queued",
                    null, null, null, Map.of("reason", "resume"), now));
            }
            return queued;
        });
    }

    /**
     * Re-queuing a transcript that already ends at a natural stop cannot create another provider
     * turn by itself. Persist one synthetic steering input for an explicit user resume so the
     * worker crosses a new model boundary. A queued retry keeps the same pending input, preserving
     * idempotency when dispatch fails after the state transition.
     */
    private HarnessRunState enqueueIncompletePlanResumeInput(HarnessRunState run, long now) {
        PlanAggregate plan = run.executionPlan();
        if (run.status() != HarnessRunStatus.SUSPENDED
            || !INCOMPLETE_PLAN_SUSPENSION.equals(run.error())
            || plan == null
            || (plan.mode() != ExecutionMode.BUILD && plan.mode() != ExecutionMode.VERIFY)
            || run.pendingInputs().stream().anyMatch(input ->
                input.kind() == HarnessInputKind.STEER
                    || input.kind() == HarnessInputKind.FOLLOW_UP)) {
            return run;
        }
        String instruction = "The user explicitly resumed this run after the model stopped before "
            + "the authoritative plan completed. Continue from the current " + plan.mode().name()
            + " state now: use the available tools, persist plan evidence and progress, and do not "
            + "stop after narration alone.";
        requirePendingInputCapacity(run, instruction);
        String inputId = stableId("input", run.owner(), run.sessionId(),
            run.runId() + "\u0000plan-resume\u0000" + run.revision());
        return run.enqueue(HarnessQueuedInput.createWithId(inputId, HarnessInputKind.STEER,
            instruction, now), now);
    }

    /** Resolves control-plane approval state only; the queued worker owns any later execution. */
    public HarnessRunState resolveToolApproval(HarnessOwner owner, String sessionId, String runId,
                                               String approvalId, String decisionId,
                                               ApprovalDecision decision, long expectedRevision,
                                               String argumentsSha256, String note) {
        return sessionGate.withSession(owner, sessionId, () -> {
            HarnessRunState run = getRun(owner, sessionId, runId);
            ToolCallApprovalAggregate approval = run.toolApprovals().get(approvalId);
            if (approval == null) {
                throw new HarnessNotFoundException("tool approval", approvalId);
            }
            long now = System.currentTimeMillis();
            ToolCallApprovalAggregate resolved;
            try {
                resolved = approval.resolve(new ResolveApprovalCommand(
                    decisionId, decision, expectedRevision, argumentsSha256, owner, sessionId,
                    note), now);
            } catch (IllegalStateException conflict) {
                throw new HarnessConflictException(conflict.getMessage(), conflict);
            }
            boolean newlyResolved = resolved != approval;
            HarnessRunState next = newlyResolved ? run.withToolApproval(resolved, now) : run;

            HarnessApproval preview = run.approvals().get(approvalId);
            if (newlyResolved && preview != null) {
                HarnessApprovalStatus status = decision == ApprovalDecision.APPROVE
                    ? HarnessApprovalStatus.APPROVED_ONCE : HarnessApprovalStatus.DENIED;
                next = next.withApproval(new HarnessApproval(preview.approvalId(),
                    preview.toolCallId(), preview.toolName(), preview.capability(),
                    preview.summary(), preview.argumentsPreview(), status, preview.createdAt(),
                    now, owner.userId(), note), now);
            }

            boolean pending = next.toolApprovals().values().stream()
                .anyMatch(item -> item.state() == ApprovalState.PENDING);
            boolean shouldSchedule = next.status() == HarnessRunStatus.WAITING_FOR_APPROVAL
                && !pending;
            if (shouldSchedule) {
                next = next.transition(HarnessRunStatus.QUEUED, null, now);
            }
            if (newlyResolved) {
                String eventId = stableId("event", owner, sessionId,
                    runId + "\u0000approval.resolved\u0000" + approvalId
                        + "\u0000" + decisionId);
                long resolvedAt = resolved.decisionReceipt().resolvedAt();
                next = next.enqueueEvent(HarnessEvent.draftWithId(eventId, sessionId, runId,
                    "approval.resolved", null, resolved.toolCallId(), approvalId,
                    Map.of("decision", decision.name(), "state", resolved.state().name()),
                    resolvedAt), now);
            }
            HarnessRunState saved = next == run ? run
                : store.saveRun(owner, next, run.revision());
            if (saved.status() == HarnessRunStatus.QUEUED) {
                // Duplicate scheduling is suppressed by HarnessScheduler and repairs an earlier
                // post-save event failure on idempotent API retry.
                scheduler.schedule(new HarnessRunRequest(owner, sessionId, runId));
            }
            return eventOutboxService.drainBestEffort(owner, saved);
        });
    }

    /** Approves exactly one immutable taskId/revision/hash tuple; model tools cannot invoke this. */
    public HarnessRunState approvePlan(HarnessOwner owner, String sessionId, String runId,
                                       UUID taskId, long expectedRevision, String expectedHash,
                                       String idempotencyKey) {
        return sessionGate.withSession(owner, sessionId, () -> {
            HarnessRunState run = getRun(owner, sessionId, runId);
            PlanAggregate plan = run.executionPlan();
            if (plan == null) {
                throw new HarnessConflictException("Run has no authoritative execution plan");
            }
            long now = System.currentTimeMillis();
            PlanAggregate approved;
            try {
                approved = plan.approveFromControlPlane(new PlanApprovalCommand(
                    taskId, expectedRevision, expectedHash, idempotencyKey), now);
            } catch (IllegalStateException conflict) {
                throw new HarnessConflictException(conflict.getMessage(), conflict);
            }
            if (approved != plan) {
                requireStablePlanControlBoundary(owner, run);
            }
            HarnessRunState next = approved == plan ? run : run.withExecutionPlan(approved, now);
            boolean shouldSchedule = next.status() == HarnessRunStatus.WAITING_FOR_INPUT;
            if (shouldSchedule) {
                next = next.transition(HarnessRunStatus.QUEUED, null, now);
            }
            HarnessRunState saved = next == run ? run : store.saveRun(owner, next, run.revision());
            if (saved.status() == HarnessRunStatus.QUEUED) {
                scheduler.schedule(new HarnessRunRequest(owner, sessionId, runId));
            }
            if (!eventWithDataExists(owner, sessionId, runId, "plan.approved",
                "idempotencyKey", idempotencyKey)) {
                eventHub.publish(owner, HarnessEvent.draft(sessionId, runId, "plan.approved",
                    null, null, null, Map.of("taskId", approved.taskId().toString(),
                        "revision", approved.revision(), "hash", approved.canonicalHash(),
                        "idempotencyKey", idempotencyKey), now));
            }
            return saved;
        });
    }

    /** Records authenticated plan feedback and schedules a new model turn to revise the draft. */
    public HarnessRunState requestPlanRevision(HarnessOwner owner, String sessionId, String runId,
                                               UUID taskId, long expectedRevision,
                                               String expectedHash, String feedbackId,
                                               String content) {
        return sessionGate.withSession(owner, sessionId, () -> {
            HarnessRunState run = getRun(owner, sessionId, runId);
            PlanAggregate plan = run.executionPlan();
            if (plan == null) {
                throw new HarnessConflictException("Run has no authoritative execution plan");
            }
            String normalizedFeedbackId = feedbackId == null ? null : feedbackId.strip();
            var priorFeedback = plan.feedbackHistory().stream()
                .filter(item -> item.feedbackId().equals(normalizedFeedbackId))
                .findFirst().orElse(null);
            if (priorFeedback != null && !priorFeedback.content().equals(content.strip())) {
                throw new HarnessConflictException(
                    "feedbackId was already used with different plan feedback");
            }
            boolean idempotentReplay = priorFeedback != null && plan.taskId().equals(taskId);
            if (!idempotentReplay && (!plan.taskId().equals(taskId)
                || plan.revision() != expectedRevision
                || !constantTimeEquals(plan.canonicalHash(), expectedHash))) {
                throw new HarnessConflictException(
                    "Plan feedback targets a stale taskId, revision, or hash");
            }
            if (!idempotentReplay) {
                requireStablePlanControlBoundary(owner, run);
            }
            long now = System.currentTimeMillis();
            PlanAggregate revised;
            try {
                revised = plan.requestRevision(normalizedFeedbackId, content, now);
            } catch (IllegalStateException conflict) {
                throw new HarnessConflictException(conflict.getMessage(), conflict);
            }
            HarnessRunState next = revised == plan ? run : run.withExecutionPlan(revised, now);
            if (next.status() == HarnessRunStatus.WAITING_FOR_INPUT) {
                next = next.transition(HarnessRunStatus.QUEUED, null, now);
            }
            var durableFeedback = revised.feedbackHistory().stream()
                .filter(item -> item.feedbackId().equals(normalizedFeedbackId))
                .findFirst().orElseThrow(() -> new IllegalStateException(
                    "Plan revision did not retain its durable feedback"));
            if (revised != plan) {
                String eventId = stableId("event", owner, sessionId,
                    runId + "\u0000plan.revision.requested\u0000" + normalizedFeedbackId);
                next = next.enqueueEvent(HarnessEvent.draftWithId(eventId, sessionId, runId,
                    "plan.revision.requested", null, null, null,
                    Map.of("taskId", taskId.toString(),
                        "feedbackId", normalizedFeedbackId,
                        "revision", revised.revision(), "hash", revised.canonicalHash()),
                    durableFeedback.createdAt()), now);
            }
            HarnessRunState saved = next == run ? run : store.saveRun(owner, next, run.revision());
            if (saved.status() == HarnessRunStatus.QUEUED) {
                scheduler.schedule(new HarnessRunRequest(owner, sessionId, runId));
            }
            return eventOutboxService.drainBestEffort(owner, saved);
        });
    }

    private void requireStablePlanControlBoundary(HarnessOwner owner, HarnessRunState run) {
        if (run.status() != HarnessRunStatus.WAITING_FOR_INPUT) {
            throw new HarnessConflictException(
                "Plan control commands require a stable WAITING_FOR_INPUT boundary");
        }
        if (run.modelEffect() != null
            && run.modelEffect().status() == org.ruoyi.service.coding.harness.model.HarnessModelEffectStatus.PENDING) {
            throw new HarnessConflictException(
                "Plan control commands cannot race an active provider request");
        }
        // Plan approval is a control-plane decision over the durable run ledger, not a model
        // context projection. A checkpoint may compact through an ASSISTANT tool-call message
        // while its adjacent TOOL result remains after the checkpoint. Validating only the
        // post-checkpoint suffix would then manufacture an ORPHAN_RESULT and permanently block
        // an otherwise closed approval boundary. Rebuild the bounded, run-scoped ledger from
        // sequence zero so adjacency is evaluated against the authoritative transcript.
        List<HarnessMessage> runMessages = transcriptReader.readAfter(owner, run.sessionId(), 0)
            .stream()
            .filter(message -> run.runId().equals(message.runId()))
            .toList();
        ToolProtocolValidation validation = toolProtocolValidator.validate(runMessages);
        if (!validation.violations().isEmpty() || validation.lastUnclosedBatch().isPresent()) {
            throw new HarnessConflictException(
                "Plan control commands require a closed, valid tool protocol boundary");
        }
    }

    public List<HarnessMessage> readMessages(HarnessOwner owner, String sessionId,
                                             long afterSequence, int limit) {
        getSession(owner, sessionId);
        return store.readMessages(owner, sessionId, afterSequence, limit);
    }

    public List<HarnessEvent> readEvents(HarnessOwner owner, String sessionId, String runId,
                                         long afterSequence, int limit) {
        HarnessRunState run = getRun(owner, sessionId, runId);
        if (run.status().isTerminal()) {
            eventHub.ensureTerminalEvent(owner, sessionId, runId);
        }
        return store.readEvents(owner, sessionId, runId, afterSequence, limit);
    }

    private void requireNoActiveRun(HarnessOwner owner, HarnessSessionState session) {
        if (session.activeRunId() == null) {
            return;
        }
        HarnessRunState active = store.findRun(owner, session.sessionId(), session.activeRunId())
            .orElse(null);
        if (active != null && !active.status().isTerminal()) {
            throw new HarnessConflictException("Session already has an active run: " + active.runId());
        }
    }

    private HarnessRunState abandonPendingToolEffects(HarnessRunState run, String reason,
                                                       long now) {
        HarnessRunState next = run;
        for (HarnessToolEffect effect : run.toolEffects().values()) {
            if (effect.status() == HarnessToolEffectStatus.PENDING) {
                next = next.withToolEffect(effect.abandon(reason, now), now);
            }
        }
        return next;
    }

    private void requirePendingInputCapacity(HarnessRunState run, String content) {
        if (run.pendingInputs().size() >= MAX_PENDING_INPUTS_PER_RUN) {
            throw new HarnessConflictException("Pending input queue limit exceeded");
        }
        long bytes = content.getBytes(StandardCharsets.UTF_8).length;
        for (HarnessQueuedInput pending : run.pendingInputs()) {
            bytes += pending.content().getBytes(StandardCharsets.UTF_8).length;
            if (bytes > MAX_PENDING_INPUT_BYTES_PER_RUN) {
                break;
            }
        }
        if (bytes > MAX_PENDING_INPUT_BYTES_PER_RUN) {
            throw new HarnessConflictException("Pending input byte budget exceeded");
        }
    }

    /** Repairs every durable stage that can be left incomplete by a process crash or event outage. */
    private HarnessRunState repairIdempotentRunCreation(HarnessOwner owner,
                                                        HarnessSessionState session,
                                                        HarnessRunState run) {
        long now = System.currentTimeMillis();
        if (!run.status().isTerminal() && !run.runId().equals(session.activeRunId())) {
            if (session.activeRunId() != null) {
                HarnessRunState active = store.findRun(owner, session.sessionId(),
                    session.activeRunId()).orElse(null);
                if (active != null && !active.status().isTerminal()) {
                    throw new HarnessConflictException(
                        "Session already has an active run: " + active.runId());
                }
            }
            session = store.saveSession(owner, session.withActiveRun(run.runId(), now),
                session.revision());
        }

        appendInitialMessageIfMissing(owner, run, now);
        if (run.status() == HarnessRunStatus.QUEUED) {
            scheduler.schedule(new HarnessRunRequest(owner, run.sessionId(), run.runId()));
        }
        if (!eventTypeExists(owner, run.sessionId(), run.runId(), "run.created")) {
            eventHub.publish(owner, HarnessEvent.draft(run.sessionId(), run.runId(),
                "run.created", null, null, null, Map.of("status", run.status().name()), now));
        }
        return run;
    }

    private void appendInitialMessageIfMissing(HarnessOwner owner, HarnessRunState run, long now) {
        String inputId = "run-create:" + run.runId();
        if (messageWithInputIdExists(owner, run.sessionId(), inputId)) {
            return;
        }
        store.appendMessage(owner, HarnessMessage.draft(run.sessionId(), run.runId(),
            HarnessMessageRole.USER, run.originalRequirement(), null, List.of(), null, null,
            false, null, Map.of("kind", HarnessInputKind.INITIAL.name(), "inputId", inputId), now));
    }

    private boolean eventTypeExists(HarnessOwner owner, String sessionId, String runId,
                                    String eventType) {
        long cursor = 0;
        int inspected = 0;
        while (inspected < 100_000) {
            List<HarnessEvent> page = store.readEvents(owner, sessionId, runId, cursor, 1_000);
            if (page.isEmpty()) {
                return false;
            }
            for (HarnessEvent event : page) {
                if (eventType.equals(event.type())) {
                    return true;
                }
                cursor = event.sequence();
                inspected++;
            }
            if (page.size() < 1_000) {
                return false;
            }
        }
        throw new HarnessConflictException("Run event ledger exceeds idempotency scan limit");
    }

    private boolean eventWithDataExists(HarnessOwner owner, String sessionId, String runId,
                                        String eventType, String key, Object expectedValue) {
        long cursor = 0;
        int inspected = 0;
        while (inspected < 100_000) {
            List<HarnessEvent> page = store.readEvents(owner, sessionId, runId, cursor, 1_000);
            if (page.isEmpty()) {
                return false;
            }
            for (HarnessEvent event : page) {
                if (eventType.equals(event.type())
                    && Objects.equals(expectedValue, event.data().get(key))) {
                    return true;
                }
                cursor = event.sequence();
                inspected++;
            }
            if (page.size() < 1_000) {
                return false;
            }
        }
        throw new HarnessConflictException("Run event ledger exceeds idempotency scan limit");
    }

    private boolean messageWithInputIdExists(HarnessOwner owner, String sessionId,
                                             String inputId) {
        return findMessageByInputId(owner, sessionId, inputId) != null;
    }

    private HarnessMessage findMessageByInputId(HarnessOwner owner, String sessionId,
                                                String inputId) {
        long cursor = 0;
        int inspected = 0;
        while (inspected < 100_000) {
            List<HarnessMessage> page = store.readMessages(owner, sessionId, cursor, 1_000);
            if (page.isEmpty()) {
                return null;
            }
            for (HarnessMessage message : page) {
                if (inputId.equals(message.metadata().get("inputId"))) {
                    return message;
                }
                cursor = message.sequence();
                inspected++;
            }
            if (page.size() < 1_000) {
                return null;
            }
        }
        throw new HarnessConflictException("Session message ledger exceeds feedback scan limit");
    }

    private boolean constantTimeEquals(String actual, String expected) {
        if (actual == null || expected == null) {
            return false;
        }
        return MessageDigest.isEqual(actual.getBytes(StandardCharsets.US_ASCII),
            expected.strip().toLowerCase().getBytes(StandardCharsets.US_ASCII));
    }

    private String stableId(String domain, HarnessOwner owner, String sessionId,
                            String idempotencyKey) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            updateDigest(digest, domain);
            updateDigest(digest, owner.tenantId());
            updateDigest(digest, owner.userId().toString());
            updateDigest(digest, sessionId == null ? "" : sessionId);
            updateDigest(digest, idempotencyKey);
            byte[] value = digest.digest();
            StringBuilder encoded = new StringBuilder(50);
            encoded.append(switch (domain) {
                case "session" -> "s_";
                case "run" -> "r_";
                case "input" -> "i_";
                case "event" -> "e_";
                default -> throw new IllegalArgumentException("Unknown stable id domain");
            });
            for (int index = 0; index < 24; index++) {
                encoded.append(Character.forDigit((value[index] >>> 4) & 0xf, 16));
                encoded.append(Character.forDigit(value[index] & 0xf, 16));
            }
            return encoded.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private void updateDigest(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update((byte) (bytes.length >>> 24));
        digest.update((byte) (bytes.length >>> 16));
        digest.update((byte) (bytes.length >>> 8));
        digest.update((byte) bytes.length);
        digest.update(bytes);
    }

    private Path canonicalWorkspace(HarnessOwner owner, String requested) {
        Path root = workspaceService.resolveHarnessRoot(owner, requested);
        try {
            Files.createDirectories(root);
            return root.toRealPath();
        } catch (IOException error) {
            throw new IllegalArgumentException("Cannot open workspace: " + root, error);
        }
    }
}
