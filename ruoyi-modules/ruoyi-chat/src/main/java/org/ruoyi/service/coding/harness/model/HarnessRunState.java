package org.ruoyi.service.coding.harness.model;

import org.ruoyi.service.coding.harness.approval.ToolCallApprovalAggregate;
import org.ruoyi.service.coding.harness.context.CompactionControl;
import org.ruoyi.service.coding.harness.plan.PlanAggregate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Durable control-plane snapshot for one execution of a session. */
public record HarnessRunState(
    int schemaVersion,
    String runId,
    String sessionId,
    String tenantId,
    Long userId,
    HarnessRunStatus status,
    String originalRequirement,
    HarnessPermissionMode permissionMode,
    long permissionRevision,
    HarnessBudget budget,
    HarnessPlan plan,
    PlanAggregate executionPlan,
    List<HarnessQueuedInput> pendingInputs,
    Map<String, HarnessApproval> approvals,
    Map<String, ToolCallApprovalAggregate> toolApprovals,
    Map<String, HarnessToolEffect> toolEffects,
    HarnessInspectionLedger inspectionLedger,
    HarnessContextCheckpoint contextCheckpoint,
    CompactionControl compactionControl,
    HarnessUsage cumulativeUsage,
    boolean usageInitialized,
    HarnessModelEffect modelEffect,
    List<HarnessEventOutboxEntry> eventOutbox,
    int iteration,
    int toolCallCount,
    boolean cancellationRequested,
    String error,
    long createdAt,
    long updatedAt,
    long revision
) {

    public static final int CURRENT_SCHEMA_VERSION = 5;
    public static final int MAX_EVENT_OUTBOX_ENTRIES = 128;

    public HarnessRunState {
        pendingInputs = pendingInputs == null ? List.of() : List.copyOf(pendingInputs);
        approvals = approvals == null ? Map.of() : Map.copyOf(approvals);
        toolApprovals = toolApprovals == null ? Map.of() : Map.copyOf(toolApprovals);
        toolEffects = toolEffects == null ? Map.of() : Map.copyOf(toolEffects);
        inspectionLedger = inspectionLedger == null ? HarnessInspectionLedger.empty() : inspectionLedger;
        eventOutbox = eventOutbox == null ? List.of() : List.copyOf(eventOutbox);
        plan = plan == null ? HarnessPlan.empty() : plan;
        contextCheckpoint = contextCheckpoint == null ? HarnessContextCheckpoint.empty() : contextCheckpoint;
        compactionControl = compactionControl == null ? CompactionControl.initial() : compactionControl;
        cumulativeUsage = cumulativeUsage == null ? HarnessUsage.empty() : cumulativeUsage;
        budget = budget == null ? HarnessBudget.defaults() : budget;
        if (schemaVersion < 1 || runId == null || runId.isBlank() || sessionId == null || sessionId.isBlank()
            || tenantId == null || tenantId.isBlank() || userId == null || userId <= 0 || status == null
            || originalRequirement == null || originalRequirement.isBlank() || permissionMode == null
            || permissionRevision < 0 || iteration < 0 || toolCallCount < 0 || createdAt <= 0
            || updatedAt <= 0 || revision < 0) {
            throw new IllegalArgumentException("Invalid Harness run state");
        }
        if (!usageInitialized && (cumulativeUsage.inputTokens() != 0
            || cumulativeUsage.outputTokens() != 0 || cumulativeUsage.totalTokens() != 0)) {
            throw new IllegalArgumentException(
                "Uninitialized Harness usage cannot contain accounted tokens");
        }
        if (executionPlan != null
            && !originalRequirement.equals(executionPlan.originalRequest())) {
            throw new IllegalArgumentException("Execution plan changed the original requirement");
        }
        // A compact record constructor runs before the implicit field assignments. Calling
        // owner() here would therefore read null fields even though the constructor parameters are
        // valid. Validate nested aggregates against an owner built from those parameters instead.
        HarnessOwner aggregateOwner = new HarnessOwner(tenantId, userId);
        toolApprovals.forEach((approvalId, approval) -> {
            if (approvalId == null || approval == null
                || !approvalId.equals(approval.approvalId())
                || !runId.equals(approval.runId())
                || !sessionId.equals(approval.sessionId())
                || !aggregateOwner.equals(approval.owner())) {
                throw new IllegalArgumentException("Tool approval does not belong to this run");
            }
        });
        toolEffects.forEach((toolCallId, effect) -> {
            if (toolCallId == null || effect == null
                || !toolCallId.equals(effect.toolCallId())) {
                throw new IllegalArgumentException("Tool effect key does not match its call");
            }
        });
        if (eventOutbox.size() > MAX_EVENT_OUTBOX_ENTRIES) {
            throw new IllegalArgumentException("Harness event outbox capacity exceeded");
        }
        Set<String> eventIds = new HashSet<>();
        for (HarnessEventOutboxEntry entry : eventOutbox) {
            if (entry == null || !entry.belongsTo(sessionId, runId)
                || !eventIds.add(entry.event().eventId())) {
                throw new IllegalArgumentException(
                    "Event outbox entry does not belong uniquely to this run");
            }
        }
    }

    public static HarnessRunState create(HarnessSessionState session, String requirement,
                                         HarnessBudget budget, long now) {
        return createWithId(UUID.randomUUID().toString(), session, requirement, budget, now);
    }

    public static HarnessRunState createWithId(String runId, HarnessSessionState session,
                                               String requirement, HarnessBudget budget, long now) {
        return new HarnessRunState(CURRENT_SCHEMA_VERSION, runId,
            session.sessionId(), session.tenantId(), session.userId(), HarnessRunStatus.QUEUED,
            requirement, session.permissionMode(), session.revision(),
            budget == null ? HarnessBudget.defaults() : budget,
            HarnessPlan.empty(), null, List.of(), Map.of(), Map.of(), Map.of(),
            HarnessInspectionLedger.empty(),
            HarnessContextCheckpoint.empty(),
            CompactionControl.initial(), HarnessUsage.empty(), true, null, List.of(),
            0, 0, false, null, now, now, 0);
    }

    public HarnessOwner owner() {
        return new HarnessOwner(tenantId, userId);
    }

    public HarnessRunState transition(HarnessRunStatus target, String transitionError, long now) {
        if (!status.canTransitionTo(target)) {
            throw new IllegalStateException("Invalid run transition: " + status + " -> " + target);
        }
        return copy(target, plan, pendingInputs, approvals, contextCheckpoint, iteration,
            toolCallCount, cancellationRequested, transitionError, now, revision);
    }

    public HarnessRunState withPlan(HarnessPlan newPlan, long now) {
        return copy(status, newPlan, pendingInputs, approvals, contextCheckpoint, iteration,
            toolCallCount, cancellationRequested, error, now, revision);
    }

    /** Stores the authoritative execution contract; {@link #plan()} remains a UI projection. */
    public HarnessRunState withExecutionPlan(PlanAggregate newExecutionPlan, long now) {
        if (newExecutionPlan == null
            || !originalRequirement.equals(newExecutionPlan.originalRequest())) {
            throw new IllegalArgumentException("Execution plan must preserve the original requirement");
        }
        return copy(status, plan, newExecutionPlan, pendingInputs, approvals, toolApprovals,
            toolEffects, contextCheckpoint, compactionControl, cumulativeUsage, usageInitialized,
            modelEffect, eventOutbox, iteration, toolCallCount,
            cancellationRequested, error, now, revision);
    }

    public HarnessRunState enqueue(HarnessQueuedInput input, long now) {
        List<HarnessQueuedInput> next = new ArrayList<>(pendingInputs);
        next.add(input);
        return copy(status, plan, next, approvals, contextCheckpoint, iteration,
            toolCallCount, cancellationRequested, error, now, revision);
    }

    public HarnessRunState withPendingInputs(List<HarnessQueuedInput> inputs, long now) {
        return copy(status, plan, inputs, approvals, contextCheckpoint, iteration,
            toolCallCount, cancellationRequested, error, now, revision);
    }

    public HarnessRunState withApproval(HarnessApproval approval, long now) {
        Map<String, HarnessApproval> next = new LinkedHashMap<>(approvals);
        next.put(approval.approvalId(), approval);
        return copy(status, plan, pendingInputs, next, contextCheckpoint, iteration,
            toolCallCount, cancellationRequested, error, now, revision);
    }

    public HarnessRunState withToolApproval(ToolCallApprovalAggregate approval, long now) {
        if (approval == null || !runId.equals(approval.runId())
            || !sessionId.equals(approval.sessionId()) || !owner().equals(approval.owner())) {
            throw new IllegalArgumentException("Tool approval does not belong to this run");
        }
        Map<String, ToolCallApprovalAggregate> next = new LinkedHashMap<>(toolApprovals);
        next.put(approval.approvalId(), approval);
        return copy(status, plan, executionPlan, pendingInputs, approvals, next, toolEffects,
            contextCheckpoint, compactionControl, cumulativeUsage, usageInitialized, modelEffect,
            eventOutbox, iteration, toolCallCount,
            cancellationRequested, error, now, revision);
    }

    public HarnessRunState withToolEffect(HarnessToolEffect effect, long now) {
        if (effect == null) {
            throw new IllegalArgumentException("Tool effect is required");
        }
        Map<String, HarnessToolEffect> next = new LinkedHashMap<>(toolEffects);
        HarnessToolEffect existing = next.get(effect.toolCallId());
        if (existing != null && (!existing.effectId().equals(effect.effectId())
            || !existing.argumentsSha256().equals(effect.argumentsSha256())
            || !existing.toolName().equals(effect.toolName()))) {
            throw new IllegalArgumentException("Tool effect identity cannot be replaced");
        }
        next.put(effect.toolCallId(), effect);
        return copy(status, plan, executionPlan, pendingInputs, approvals, toolApprovals, next,
            contextCheckpoint, compactionControl, cumulativeUsage, usageInitialized, modelEffect,
            eventOutbox, iteration, toolCallCount,
            cancellationRequested, error, now, revision);
    }

    public HarnessRunState withInspectionLedger(HarnessInspectionLedger ledger, long now) {
        if (ledger == null) {
            throw new IllegalArgumentException("Harness inspection ledger is required");
        }
        return new HarnessRunState(Math.max(schemaVersion, CURRENT_SCHEMA_VERSION), runId,
            sessionId, tenantId, userId, status, originalRequirement, permissionMode,
            permissionRevision, budget, plan, executionPlan, pendingInputs, approvals,
            toolApprovals, toolEffects, ledger, contextCheckpoint, compactionControl,
            cumulativeUsage, usageInitialized, modelEffect, eventOutbox, iteration,
            toolCallCount, cancellationRequested, error, createdAt, now, revision);
    }

    public HarnessRunState withContextCheckpoint(HarnessContextCheckpoint checkpoint, long now) {
        return copy(status, plan, pendingInputs, approvals, checkpoint, iteration,
            toolCallCount, cancellationRequested, error, now, revision);
    }

    public HarnessRunState withContextState(HarnessContextCheckpoint checkpoint,
                                            CompactionControl control, long now) {
        return copy(status, plan, executionPlan, pendingInputs, approvals, toolApprovals,
            toolEffects, checkpoint, control, cumulativeUsage, usageInitialized, modelEffect,
            eventOutbox, iteration, toolCallCount,
            cancellationRequested, error, now, revision);
    }

    /**
     * Persists the one-time migration result for snapshots written before cumulative usage was
     * part of the run state. New runs are initialized at creation and never need a ledger scan.
     */
    public HarnessRunState withCumulativeUsage(HarnessUsage usage, long now) {
        if (usage == null) {
            throw new IllegalArgumentException("Harness cumulative usage is required");
        }
        return copy(status, plan, executionPlan, pendingInputs, approvals, toolApprovals,
            toolEffects, contextCheckpoint, compactionControl, usage, true, modelEffect,
            eventOutbox, iteration, toolCallCount, cancellationRequested, error, now, revision);
    }

    /** Adds one durably admitted model response exactly once at model-effect settlement. */
    public HarnessRunState addModelUsage(HarnessUsage usage, long now) {
        if (!usageInitialized) {
            throw new IllegalStateException(
                "Legacy Harness usage must be initialized before recording a response");
        }
        if (usage == null) {
            throw new IllegalArgumentException("Harness model usage is required");
        }
        HarnessUsage next = new HarnessUsage(
            saturatingAdd(cumulativeUsage.inputTokens(), usage.inputTokens()),
            saturatingAdd(cumulativeUsage.outputTokens(), usage.outputTokens()),
            saturatingAdd(cumulativeUsage.totalTokens(), usage.totalTokens()));
        return withCumulativeUsage(next, now);
    }

    public HarnessRunState withModelEffect(HarnessModelEffect effect, long now) {
        if (effect != null && effect.iteration() != iteration) {
            throw new IllegalArgumentException("Model effect iteration does not match run state");
        }
        return copy(status, plan, executionPlan, pendingInputs, approvals, toolApprovals,
            toolEffects, contextCheckpoint, compactionControl, cumulativeUsage, usageInitialized,
            effect, eventOutbox, iteration, toolCallCount,
            cancellationRequested, error, now, revision);
    }

    /** Adds one immutable draft before the associated business mutation is saved. */
    public HarnessRunState enqueueEvent(HarnessEvent event, long now) {
        if (event == null || event.sequence() != 0
            || !sessionId.equals(event.sessionId()) || !runId.equals(event.runId())) {
            throw new IllegalArgumentException("Event draft does not belong to this run");
        }
        for (HarnessEventOutboxEntry entry : eventOutbox) {
            if (!entry.event().eventId().equals(event.eventId())) {
                continue;
            }
            if (entry.event().equals(event)) {
                return this;
            }
            throw new IllegalArgumentException(
                "Event id is already bound to a different outbox payload");
        }
        if (eventOutbox.size() >= MAX_EVENT_OUTBOX_ENTRIES) {
            throw new IllegalStateException("Harness event outbox capacity exceeded");
        }
        List<HarnessEventOutboxEntry> next = new ArrayList<>(eventOutbox);
        next.add(HarnessEventOutboxEntry.create(event, now));
        return copy(status, plan, executionPlan, pendingInputs, approvals, toolApprovals,
            toolEffects, contextCheckpoint, compactionControl, cumulativeUsage, usageInitialized,
            modelEffect, next, iteration, toolCallCount, cancellationRequested, error, now,
            revision);
    }

    /** Removes a draft only after the event ledger has acknowledged its stable event id. */
    public HarnessRunState acknowledgeEvent(String eventId, long now) {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId is required");
        }
        List<HarnessEventOutboxEntry> next = eventOutbox.stream()
            .filter(entry -> !entry.event().eventId().equals(eventId))
            .toList();
        if (next.size() == eventOutbox.size()) {
            return this;
        }
        return copy(status, plan, executionPlan, pendingInputs, approvals, toolApprovals,
            toolEffects, contextCheckpoint, compactionControl, cumulativeUsage, usageInitialized,
            modelEffect, next, iteration, toolCallCount, cancellationRequested, error, now,
            revision);
    }

    public HarnessRunState withCounters(int newIteration, int newToolCallCount, long now) {
        return copy(status, plan, pendingInputs, approvals, contextCheckpoint, newIteration,
            newToolCallCount, cancellationRequested, error, now, revision);
    }

    public HarnessRunState requestCancellation(long now) {
        if (status.isTerminal()) {
            return this;
        }
        return copy(status, plan, pendingInputs, approvals, contextCheckpoint, iteration,
            toolCallCount, true, error, now, revision);
    }

    public HarnessRunState withRevision(long newRevision) {
        return copy(status, plan, pendingInputs, approvals, contextCheckpoint, iteration,
            toolCallCount, cancellationRequested, error, updatedAt, newRevision);
    }

    private HarnessRunState copy(HarnessRunStatus newStatus, HarnessPlan newPlan,
                                 List<HarnessQueuedInput> newInputs,
                                 Map<String, HarnessApproval> newApprovals,
                                 HarnessContextCheckpoint checkpoint, int newIteration,
                                 int newToolCallCount, boolean cancelRequested,
                                 String newError, long now, long newRevision) {
        return copy(newStatus, newPlan, executionPlan, newInputs, newApprovals, toolApprovals,
            toolEffects, checkpoint, compactionControl, cumulativeUsage, usageInitialized,
            modelEffect, eventOutbox, newIteration, newToolCallCount, cancelRequested, newError, now,
            newRevision);
    }

    private HarnessRunState copy(HarnessRunStatus newStatus, HarnessPlan newPlan,
                                 PlanAggregate newExecutionPlan,
                                 List<HarnessQueuedInput> newInputs,
                                 Map<String, HarnessApproval> newApprovals,
                                 Map<String, ToolCallApprovalAggregate> newToolApprovals,
                                 Map<String, HarnessToolEffect> newToolEffects,
                                 HarnessContextCheckpoint checkpoint,
                                 CompactionControl newCompactionControl,
                                 HarnessUsage newCumulativeUsage,
                                 boolean newUsageInitialized,
                                 HarnessModelEffect newModelEffect,
                                 List<HarnessEventOutboxEntry> newEventOutbox,
                                 int newIteration,
                                 int newToolCallCount, boolean cancelRequested,
                                 String newError, long now, long newRevision) {
        return new HarnessRunState(Math.max(schemaVersion, CURRENT_SCHEMA_VERSION), runId,
            sessionId, tenantId, userId,
            newStatus, originalRequirement, permissionMode, permissionRevision, budget, newPlan,
            newExecutionPlan, newInputs, newApprovals, newToolApprovals, newToolEffects,
            inspectionLedger, checkpoint,
            newCompactionControl, newCumulativeUsage, newUsageInitialized, newModelEffect,
            newEventOutbox, newIteration, newToolCallCount, cancelRequested, newError, createdAt, now,
            newRevision);
    }

    private static long saturatingAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }
}
