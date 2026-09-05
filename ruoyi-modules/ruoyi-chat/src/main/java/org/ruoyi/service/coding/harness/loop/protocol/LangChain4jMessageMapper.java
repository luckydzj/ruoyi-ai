package org.ruoyi.service.coding.harness.loop.protocol;

import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.ToolExecutionResultMessage;
import dev.langchain4j.data.message.UserMessage;
import org.ruoyi.service.coding.harness.model.HarnessMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Lossless semantic adapter from the durable Harness ledger to LangChain4j messages. */
public final class LangChain4jMessageMapper {

    private final ToolProtocolValidator validator;

    public LangChain4jMessageMapper() {
        this(new ToolProtocolValidator());
    }

    public LangChain4jMessageMapper(ToolProtocolValidator validator) {
        this.validator = Objects.requireNonNull(validator, "validator");
    }

    /**
     * Validates, removes CONTROL records, orders tool results by source call order, and maps the
     * exact request transcript. Incomplete or malformed batches never reach LangChain4j.
     */
    public List<ChatMessage> mapForNextModelRequest(List<HarnessMessage> ledgerMessages) {
        ToolProtocolValidation validation = validator.validate(ledgerMessages);
        if (!validation.allowsNextModelRequest()) {
            throw new ToolProtocolException(validation);
        }
        List<ChatMessage> mapped = new ArrayList<>(validation.modelMessages().size());
        for (HarnessMessage message : validation.modelMessages()) {
            mapped.add(map(message));
        }
        return List.copyOf(mapped);
    }

    public ToolProtocolValidation validate(List<HarnessMessage> ledgerMessages) {
        return validator.validate(ledgerMessages);
    }

    private ChatMessage map(HarnessMessage message) {
        return switch (message.role()) {
            case SYSTEM -> SystemMessage.from(message.content());
            case USER -> UserMessage.from(message.content());
            case ASSISTANT -> AiMessage.builder()
                .text(message.content())
                .thinking(message.thinking())
                .toolExecutionRequests(message.toolCalls().stream()
                    .map(call -> ToolExecutionRequest.builder()
                        .id(call.toolCallId())
                        .name(call.toolName())
                        .arguments(call.arguments())
                        .build())
                    .toList())
                .build();
            case TOOL -> ToolExecutionResultMessage.builder()
                .id(message.toolCallId())
                .toolName(message.toolName())
                .text(message.content())
                .isError(message.toolError())
                .build();
            case CONTROL -> throw new IllegalArgumentException(
                "CONTROL records cannot be mapped to model messages");
        };
    }
}
