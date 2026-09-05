package org.ruoyi.service.coding.harness.loop;

import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.output.TokenUsage;
import org.ruoyi.service.coding.harness.model.HarnessMessage;
import org.ruoyi.service.coding.harness.model.HarnessMessageRole;
import org.ruoyi.service.coding.harness.model.HarnessRunState;
import org.ruoyi.service.coding.harness.model.HarnessToolCall;
import org.ruoyi.service.coding.harness.model.HarnessUsage;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.nio.charset.StandardCharsets;

/** Losslessly converts the provider-assembled response into the durable application ledger. */
public final class HarnessAssistantMessageMapper {

    public HarnessMessage map(ChatResponse response, HarnessRunState run,
                              String effectId, long fallbackInputTokens, long now) {
        if (response == null || response.aiMessage() == null || run == null
            || effectId == null || effectId.isBlank()) {
            throw new IllegalArgumentException("Response, run and model effect are required");
        }
        AiMessage message = response.aiMessage();
        List<HarnessToolCall> toolCalls = message.toolExecutionRequests().stream()
            .map(this::mapToolCall)
            .toList();
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("effectId", effectId);
        put(metadata, "providerResponseId", response.id());
        put(metadata, "modelName", response.modelName());
        if (response.finishReason() != null) {
            metadata.put("finishReason", response.finishReason().name());
        }
        String visibleText = message.text();
        if (response.finishReason() != null
            && "LENGTH".equals(response.finishReason().name())
            && (visibleText == null || visibleText.isBlank())
            && (message.thinking() == null || message.thinking().isBlank())
            && toolCalls.isEmpty()) {
            // Keep the durable transcript protocol-valid. The processor sees finishReason=LENGTH
            // and appends a replay-safe continuation input; this text is evidence of truncation,
            // never a fabricated model answer.
            visibleText = "[Provider response truncated before producing an actionable result.]";
            metadata.put("truncatedResponseMarker", true);
        }
        TokenUsage providerUsage = response.tokenUsage();
        if (providerUsage == null || missing(providerUsage.inputTokenCount())
            || missing(providerUsage.outputTokenCount())) {
            metadata.put("usageEstimated", true);
        }
        return HarnessMessage.draft(run.sessionId(), run.runId(), HarnessMessageRole.ASSISTANT,
            visibleText, message.thinking(), toolCalls, null, null, false,
            usage(providerUsage, fallbackInputTokens, estimateOutput(message)), metadata, now);
    }

    private HarnessToolCall mapToolCall(ToolExecutionRequest request) {
        String arguments = request.arguments();
        return new HarnessToolCall(request.id(), request.name(),
            arguments == null || arguments.isBlank() ? "{}" : arguments);
    }

    private HarnessUsage usage(TokenUsage usage, long fallbackInput, long fallbackOutput) {
        long input = usage == null || missing(usage.inputTokenCount())
            ? Math.max(1, fallbackInput) : nonNegative(usage.inputTokenCount());
        long output = usage == null || missing(usage.outputTokenCount())
            ? Math.max(1, fallbackOutput) : nonNegative(usage.outputTokenCount());
        long total = usage == null || usage.totalTokenCount() == null ? input + output
            : nonNegative(usage.totalTokenCount());
        return new HarnessUsage(input, output, Math.max(total, input + output));
    }

    private long estimateOutput(AiMessage message) {
        long bytes = utf8(message.text()) + utf8(message.thinking()) + 16;
        for (ToolExecutionRequest request : message.toolExecutionRequests()) {
            bytes = saturatingAdd(bytes, 24);
            bytes = saturatingAdd(bytes, utf8(request.id()));
            bytes = saturatingAdd(bytes, utf8(request.name()));
            bytes = saturatingAdd(bytes, utf8(request.arguments()));
        }
        return Math.max(1, bytes);
    }

    private long utf8(String value) {
        return value == null ? 0 : value.getBytes(StandardCharsets.UTF_8).length;
    }

    private long saturatingAdd(long left, long right) {
        return right > Long.MAX_VALUE - left ? Long.MAX_VALUE : left + right;
    }

    private long nonNegative(Integer value) {
        return value == null ? 0 : Math.max(0, value.longValue());
    }

    private boolean missing(Integer value) {
        return value == null || value <= 0;
    }

    private void put(Map<String, Object> values, String key, String value) {
        if (value != null && !value.isBlank()) {
            values.put(key, value);
        }
    }
}
