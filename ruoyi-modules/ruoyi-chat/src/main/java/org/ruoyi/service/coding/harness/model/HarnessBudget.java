package org.ruoyi.service.coding.harness.model;

/** Hard run limits. Zero disables the corresponding cumulative token limit. */
public record HarnessBudget(
    int maxIterations,
    int maxToolCalls,
    long maxInputTokens,
    long maxOutputTokens,
    long maxWallTimeMillis
) {

    public static HarnessBudget defaults() {
        return new HarnessBudget(200, 600, 0, 100_000, 6 * 60 * 60 * 1000L);
    }

    public HarnessBudget {
        if (maxIterations < 1 || maxToolCalls < 1 || maxInputTokens < 0
            || maxOutputTokens < 0 || maxWallTimeMillis < 1) {
            throw new IllegalArgumentException("Invalid Harness budget");
        }
    }
}
