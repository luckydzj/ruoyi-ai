package org.ruoyi.service.coding.harness.modelruntime;

import dev.langchain4j.model.chat.StreamingChatModel;
import org.ruoyi.service.coding.harness.model.HarnessRunState;
import org.ruoyi.service.coding.harness.model.HarnessSessionState;

public interface HarnessChatModelFactory {
    StreamingChatModel create(HarnessSessionState session, HarnessRunState run);
}
