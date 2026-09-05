package org.ruoyi.service.coding.harness.context;

/** Token-based retention policy; it deliberately contains no message-count threshold. */
public record ContextEnginePolicy(
    long summaryHeadroomTokens,
    long minimumRetainedTokens
) {

    public ContextEnginePolicy {
        if (summaryHeadroomTokens < 0 || minimumRetainedTokens < 0) {
            throw new IllegalArgumentException("Context policy values must be non-negative");
        }
    }

    public static ContextEnginePolicy defaults() {
        return new ContextEnginePolicy(2_048, 4_096);
    }
}
