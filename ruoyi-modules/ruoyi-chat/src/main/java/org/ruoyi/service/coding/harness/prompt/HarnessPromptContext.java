package org.ruoyi.service.coding.harness.prompt;

import org.ruoyi.service.coding.harness.model.HarnessPermissionMode;
import org.ruoyi.service.coding.harness.tool.ToolDescriptor;

import java.util.List;

public record HarnessPromptContext(
    String workspace,
    HarnessPermissionMode permissionMode,
    String responseLanguage,
    String projectInstructions,
    String planProjection,
    String budgetProjection,
    List<ToolDescriptor> tools,
    List<HarnessSkillMetadata> skills
) {
    public HarnessPromptContext {
        tools = tools == null ? List.of() : List.copyOf(tools);
        skills = skills == null ? List.of() : List.copyOf(skills);
        if (workspace == null || workspace.isBlank() || permissionMode == null
            || responseLanguage == null || responseLanguage.isBlank()) {
            throw new IllegalArgumentException(
                "Prompt context requires workspace, permission mode, and response language");
        }
    }
}
