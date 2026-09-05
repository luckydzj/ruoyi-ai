package org.ruoyi.service.coding.harness.modelruntime;

import dev.langchain4j.model.chat.StreamingChatModel;
import org.ruoyi.common.chat.domain.dto.request.ChatRequest;
import org.ruoyi.common.chat.domain.vo.chat.ChatModelVo;
import org.ruoyi.factory.ChatServiceFactory;
import org.ruoyi.service.chat.AbstractChatService;
import org.ruoyi.service.coding.harness.model.HarnessRunState;
import org.ruoyi.service.coding.harness.model.HarnessSessionState;
import org.springframework.stereotype.Service;

@Service
public class RuoYiHarnessChatModelFactory implements HarnessChatModelFactory {

    private final HarnessModelRegistry modelRegistry;
    private final ChatServiceFactory chatServiceFactory;

    public RuoYiHarnessChatModelFactory(HarnessModelRegistry modelRegistry,
                                        ChatServiceFactory chatServiceFactory) {
        this.modelRegistry = modelRegistry;
        this.chatServiceFactory = chatServiceFactory;
    }

    @Override
    public StreamingChatModel create(HarnessSessionState session, HarnessRunState run) {
        ChatModelVo model = modelRegistry.requireConfigured(session.model());
        AbstractChatService provider = chatServiceFactory.getOriginalService(model.getProviderCode());
        ChatRequest request = new ChatRequest();
        request.setModel(model.getModelName());
        request.setContent(run.originalRequirement());
        request.setEnableThinking(true);
        request.setUserId(run.userId());
        request.setChatModelVo(model);
        return provider.buildStreamingChatModel(model, request);
    }
}
