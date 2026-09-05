package org.ruoyi.service.coding.harness.app;

import org.ruoyi.service.coding.harness.model.HarnessApprovalPolicy;
import org.ruoyi.service.coding.harness.model.HarnessPermissionMode;

import java.util.UUID;

public record CreateHarnessSessionCommand(
    String workspacePath,
    String model,
    HarnessPermissionMode permissionMode,
    HarnessApprovalPolicy approvalPolicy,
    String title,
    String idempotencyKey
) {

    public CreateHarnessSessionCommand(String workspacePath, String model,
                                       HarnessPermissionMode permissionMode, String title) {
        this(workspacePath, model, permissionMode, HarnessApprovalPolicy.ON_REQUEST,
            title, UUID.randomUUID().toString());
    }

    public CreateHarnessSessionCommand(String workspacePath, String model,
                                       HarnessPermissionMode permissionMode, String title,
                                       String idempotencyKey) {
        this(workspacePath, model, permissionMode, HarnessApprovalPolicy.ON_REQUEST,
            title, idempotencyKey);
    }

    public CreateHarnessSessionCommand {
        approvalPolicy = approvalPolicy == null ? HarnessApprovalPolicy.ON_REQUEST : approvalPolicy;
        if (idempotencyKey == null || idempotencyKey.isBlank()
            || idempotencyKey.length() > 256) {
            throw new IllegalArgumentException("idempotencyKey is required and must not exceed 256 characters");
        }
        idempotencyKey = idempotencyKey.strip();
    }
}
