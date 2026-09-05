package org.ruoyi.service.coding.harness.tool.command;

import org.ruoyi.service.coding.harness.tool.ToolCapability;
import org.ruoyi.service.coding.harness.tool.ToolDescriptor;

import java.util.Set;

/** Security and resource metadata for the process execution tool. */
public final class CommandToolDescriptors {

    public static final ToolDescriptor EXECUTE = executeProcess(CommandToolConfig.DEFAULT);
    public static final ToolDescriptor INLINE_PROBE = inlineProbe(CommandToolConfig.DEFAULT);

    private CommandToolDescriptors() {
    }

    public static ToolDescriptor executeProcess(CommandToolConfig config) {
        long maxInput = Math.max(1L,
            (long) config.maxArgvEntries() * config.maxArgumentChars()
                + (long) config.maxArgumentChars() * 4L + 8 * 1024L);
        long maxOutput = Math.max(1L, (long) config.maxOutputBytesPerStream() * 2L);
        return new ToolDescriptor("execute_process", Set.of(ToolCapability.EXECUTE), false,
            config.maxTimeoutMs(), maxInput, maxOutput, true,
            "Executes an allowlisted argv process with optional bounded UTF-8 stdin in the "
                + "workspace lease; allowlisting is not sandboxing, and npm/git/build tools "
                + "can execute repository-controlled code; only finite commands are supported, "
                + "never servers/watchers or inline node -e/python -c source");
    }

    public static ToolDescriptor inlineProbe(CommandToolConfig config) {
        long maxInput = Math.max(1L, (long) config.maxArgumentChars() * 4L + 8 * 1024L);
        long maxOutput = Math.max(1L, (long) config.maxOutputBytesPerStream() * 2L);
        return new ToolDescriptor("run_inline_probe", Set.of(ToolCapability.EXECUTE), false,
            config.maxTimeoutMs(), maxInput, maxOutput, true,
            "Runs a bounded Node or Python assertion program literally from UTF-8 stdin; it "
                + "delegates to execute_process and adds no authority or sandbox; Node is ESM, "
                + "require/process.exit/browser globals/copied production logic are invalid");
    }
}
