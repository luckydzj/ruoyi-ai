package org.ruoyi.service.coding.harness.model;

import java.util.UUID;

/** Durable session metadata. The canonical workspace is immutable after creation. */
public record HarnessSessionState(
    int schemaVersion,
    String sessionId,
    String tenantId,
    Long userId,
    String workspace,
    String model,
    HarnessPermissionMode permissionMode,
    HarnessApprovalPolicy approvalPolicy,
    String title,
    String activeRunId,
    long createdAt,
    long updatedAt,
    long revision
) {

    public static final int CURRENT_SCHEMA_VERSION = 2;

    public HarnessSessionState {
        approvalPolicy = approvalPolicy == null ? HarnessApprovalPolicy.ON_REQUEST : approvalPolicy;
        if (schemaVersion < 1 || sessionId == null || sessionId.isBlank()
            || tenantId == null || tenantId.isBlank() || userId == null || userId <= 0
            || workspace == null || workspace.isBlank() || permissionMode == null
            || createdAt <= 0 || updatedAt <= 0 || revision < 0) {
            throw new IllegalArgumentException("Invalid Harness session state");
        }
    }

    /** Backward-compatible constructor for persisted/test callers created before approval policy existed. */
    public HarnessSessionState(int schemaVersion, String sessionId, String tenantId, Long userId,
                               String workspace, String model, HarnessPermissionMode permissionMode,
                               String title, String activeRunId, long createdAt, long updatedAt,
                               long revision) {
        this(schemaVersion, sessionId, tenantId, userId, workspace, model, permissionMode,
            HarnessApprovalPolicy.ON_REQUEST, title, activeRunId, createdAt, updatedAt, revision);
    }

    public static HarnessSessionState create(HarnessOwner owner, String workspace, String model,
                                             HarnessPermissionMode permissionMode, String title, long now) {
        return create(owner, workspace, model, permissionMode,
            HarnessApprovalPolicy.ON_REQUEST, title, now);
    }

    public static HarnessSessionState create(HarnessOwner owner, String workspace, String model,
                                             HarnessPermissionMode permissionMode,
                                             HarnessApprovalPolicy approvalPolicy,
                                             String title, long now) {
        return createWithId(UUID.randomUUID().toString(), owner, workspace, model,
            permissionMode, approvalPolicy, title, now);
    }

    public static HarnessSessionState createWithId(String sessionId, HarnessOwner owner,
                                                   String workspace, String model,
                                                   HarnessPermissionMode permissionMode,
                                                   String title, long now) {
        return createWithId(sessionId, owner, workspace, model, permissionMode,
            HarnessApprovalPolicy.ON_REQUEST, title, now);
    }

    public static HarnessSessionState createWithId(String sessionId, HarnessOwner owner,
                                                   String workspace, String model,
                                                   HarnessPermissionMode permissionMode,
                                                   HarnessApprovalPolicy approvalPolicy,
                                                   String title, long now) {
        return new HarnessSessionState(CURRENT_SCHEMA_VERSION, sessionId,
            owner.tenantId(), owner.userId(), workspace, model,
            permissionMode == null ? HarnessPermissionMode.READ_ONLY : permissionMode,
            approvalPolicy, title, null, now, now, 0);
    }

    public HarnessOwner owner() {
        return new HarnessOwner(tenantId, userId);
    }

    public HarnessSessionState withActiveRun(String runId, long now) {
        return new HarnessSessionState(schemaVersion, sessionId, tenantId, userId, workspace, model,
            permissionMode, approvalPolicy, title, runId, createdAt, now, revision);
    }

    public HarnessSessionState withTitle(String newTitle, long now) {
        return new HarnessSessionState(schemaVersion, sessionId, tenantId, userId, workspace, model,
            permissionMode, approvalPolicy, newTitle, activeRunId, createdAt, now, revision);
    }

    public HarnessSessionState withRevision(long newRevision) {
        return new HarnessSessionState(schemaVersion, sessionId, tenantId, userId, workspace, model,
            permissionMode, approvalPolicy, title, activeRunId, createdAt, updatedAt, newRevision);
    }
}
