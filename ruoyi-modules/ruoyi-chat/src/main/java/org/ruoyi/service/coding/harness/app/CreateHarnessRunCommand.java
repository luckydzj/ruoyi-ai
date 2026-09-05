package org.ruoyi.service.coding.harness.app;

import org.ruoyi.service.coding.harness.model.HarnessBudget;

import java.util.UUID;

public record CreateHarnessRunCommand(String requirement, HarnessBudget budget,
                                      String idempotencyKey) {

    public CreateHarnessRunCommand(String requirement, HarnessBudget budget) {
        this(requirement, budget, UUID.randomUUID().toString());
    }

    public CreateHarnessRunCommand {
        if (requirement == null || requirement.isBlank()) {
            throw new IllegalArgumentException("requirement is required");
        }
        if (idempotencyKey == null || idempotencyKey.isBlank()
            || idempotencyKey.length() > 256) {
            throw new IllegalArgumentException("idempotencyKey is required and must not exceed 256 characters");
        }
        idempotencyKey = idempotencyKey.strip();
    }
}
