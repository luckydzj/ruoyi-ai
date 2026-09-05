package org.ruoyi.service.coding.harness.tool.command;

import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;
import org.ruoyi.service.coding.harness.tool.builtin.RunContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Run-bound argv process tool. No command string is parsed and no shell is involved.
 *
 * <p>The executable allowlist is authorization only, never a sandbox. Programs including npm,
 * git, language runtimes, build tools, and test runners may execute repository-controlled code,
 * hooks, or plugins. Outer policy evaluation, per-call approval, an OS sandbox, and workspace
 * isolation must authorize every invocation before this object is called.</p>
 */
public final class ExecuteProcessTool {

    private final RunContext context;
    private final CommandToolConfig config;
    private final CommandWorkspaceGuard workspaceGuard;
    private final Map<String, String> environment;
    private final ExecutablePolicy executablePolicy;
    private final Semaphore processSlots;

    public ExecuteProcessTool(RunContext context) {
        this(context, CommandToolConfig.DEFAULT);
    }

    public ExecuteProcessTool(RunContext context, CommandToolConfig config) {
        this.context = Objects.requireNonNull(context, "context");
        this.config = Objects.requireNonNull(config, "config");
        this.workspaceGuard = new CommandWorkspaceGuard(context);
        this.environment = ControlledEnvironment.build(config);
        this.executablePolicy = new ExecutablePolicy(config, environment);
        this.processSlots = new Semaphore(config.maxConcurrentProcesses(), true);
    }

    public RunContext context() {
        return context;
    }

    public CommandToolConfig config() {
        return config;
    }

    @Tool(name = "execute_process", value = {
        "Execute one allowlisted program directly with an argv array inside the immutable workspace lease. "
            + "No shell parses the arguments. The allowlist is not a sandbox: npm, git, build tools, "
            + "test runners, and runtimes can still execute repository code and require outer approval. "
            + "The command must terminate: never start a dev/static server, watch mode, or another "
            + "long-lived process, especially as exitCode=0 plan evidence. Inline Node/Python source "
            + "in argv or stdin is forbidden: do not use node -e/-p or python -c/-. Use a finite "
            + "file-based command such as node --check, repository read tools for inspection, or "
            + "run_inline_probe when it is advertised."
    })
    public ProcessExecutionResult executeProcess(
        @P(name = "executable",
            value = "Allowlisted executable name or explicitly allowlisted absolute executable path",
            required = true)
        String executable,
        @P(name = "argv", value = "Argument array passed literally; never a shell command string",
            required = true)
        List<String> argv,
        @P(name = "cwd", value = "Optional existing workspace-relative working directory",
            required = false)
        String cwd,
        @P(name = "timeoutMs", value = "Optional timeout bounded by the run command limit",
            required = false)
        Long timeoutMs,
        @P(name = "stdin",
            value = "Optional bounded UTF-8 input for an existing program; inline interpreter source is forbidden",
            required = false)
        String stdin
    ) {
        return executeProcessInternal(executable, argv, cwd, timeoutMs, stdin, false);
    }

    ProcessExecutionResult executeInlineProbe(String executable, List<String> argv,
                                               String cwd, Long timeoutMs, String stdin) {
        return executeProcessInternal(executable, argv, cwd, timeoutMs, stdin, true);
    }

    private ProcessExecutionResult executeProcessInternal(String executable, List<String> argv,
                                                           String cwd, Long timeoutMs, String stdin,
                                                           boolean inlineProbe) {
        List<String> arguments = CommandValidation.validateArgv(argv, config);
        rejectDuplicatedExecutableArgument(executable, arguments);
        if (!inlineProbe) {
            rejectInlineInterpreterProgram(executable, arguments, stdin);
        }
        Path authorizedExecutable = executablePolicy.authorize(executable);
        Path workingDirectory = workspaceGuard.cwd(cwd);
        long timeout = effectiveTimeout(timeoutMs);
        List<String> command = new ArrayList<>(arguments.size() + 1);
        command.add(authorizedExecutable.toString());
        command.addAll(arguments);

        boolean acquired = false;
        Process process = null;
        Thread stdoutThread = null;
        Thread stderrThread = null;
        BoundedOutputCollector stdout = null;
        BoundedOutputCollector stderr = null;
        long started = System.nanoTime();
        try {
            if (!processSlots.tryAcquire(timeout, TimeUnit.MILLISECONDS)) {
                throw new CommandToolException("PROCESS_SLOT_TIMEOUT",
                    "Process concurrency limit remained saturated until timeout");
            }
            acquired = true;
            ProcessBuilder builder = new ProcessBuilder(command);
            builder.directory(workingDirectory.toFile());
            builder.redirectErrorStream(false);
            builder.environment().clear();
            builder.environment().putAll(environment);
            // Re-check cwd immediately before process creation to narrow link-swap races.
            workingDirectory = workspaceGuard.cwd(cwd);
            builder.directory(workingDirectory.toFile());
            process = builder.start();

            stdout = new BoundedOutputCollector(process.getInputStream(),
                config.maxOutputBytesPerStream());
            stderr = new BoundedOutputCollector(process.getErrorStream(),
                config.maxOutputBytesPerStream());
            stdoutThread = collectorThread(stdout, "stdout", process.pid());
            stderrThread = collectorThread(stderr, "stderr", process.pid());
            stdoutThread.start();
            stderrThread.start();
            writeStandardInput(process, stdin);

            long remainingTimeout = remainingTimeoutMillis(started, timeout);
            boolean exited = process.waitFor(remainingTimeout, TimeUnit.MILLISECONDS);
            boolean timedOut = !exited;
            if (timedOut) {
                terminateProcessTree(process, config.terminationGraceMs());
            }
            awaitCollectors(stdoutThread, stderrThread, stdout, stderr,
                config.terminationGraceMs());
            if (!timedOut && (stdout.failure() != null || stderr.failure() != null)) {
                IOException failure = stdout.failure() != null ? stdout.failure() : stderr.failure();
                throw new CommandToolException("OUTPUT_CAPTURE_FAILED",
                    "Process output could not be captured", failure);
            }
            int exitCode = exitValue(process);
            long durationMs = elapsedMillis(started);
            boolean truncated = stdout.truncated() || stderr.truncated();
            return new ProcessExecutionResult(exitCode, timedOut, durationMs,
                stdout.content(), stderr.content(), truncated,
                stdout.truncated(), stderr.truncated());
        } catch (InterruptedException error) {
            if (process != null) {
                terminateProcessTreeUninterruptibly(process, config.terminationGraceMs());
            }
            closeAndJoinUninterruptibly(stdoutThread, stderrThread, stdout, stderr,
                config.terminationGraceMs());
            Thread.currentThread().interrupt();
            throw new ProcessExecutionInterruptedException(error);
        } catch (IOException error) {
            if (process != null) {
                terminateProcessTreeUninterruptibly(process, config.terminationGraceMs());
            }
            throw new CommandToolException("PROCESS_START_FAILED",
                "Authorized process could not be started", error);
        } catch (CommandToolException error) {
            if (process != null && process.isAlive()) {
                terminateProcessTreeUninterruptibly(process, config.terminationGraceMs());
            }
            closeAndJoinUninterruptibly(stdoutThread, stderrThread, stdout, stderr,
                config.terminationGraceMs());
            throw error;
        } finally {
            if (acquired) {
                processSlots.release();
            }
        }
    }

    /** Backward-compatible direct Java API; LangChain4j exposes only the annotated overload. */
    public ProcessExecutionResult executeProcess(String executable, List<String> argv,
                                                 String cwd, Long timeoutMs) {
        return executeProcess(executable, argv, cwd, timeoutMs, null);
    }

    private static void rejectDuplicatedExecutableArgument(String executable,
                                                           List<String> arguments) {
        if (executable == null || arguments.isEmpty()) {
            return;
        }
        String executableName = normalizedExecutableName(executable);
        String firstArgumentName = normalizedExecutableName(arguments.get(0));
        if (!executableName.isEmpty() && executableName.equals(firstArgumentName)) {
            throw new CommandToolException("DUPLICATE_EXECUTABLE_ARGUMENT",
                "argv contains only program arguments; remove argv[0]=\""
                    + arguments.get(0) + "\" because executable=\"" + executable
                    + "\" already starts that program");
        }
    }

    private static void rejectInlineInterpreterProgram(String executable,
                                                       List<String> arguments,
                                                       String stdin) {
        String name = normalizedExecutableName(Objects.toString(executable, ""));
        boolean hasStdin = stdin != null && !stdin.isBlank();
        boolean inline = switch (name) {
            case "node" -> arguments.stream().anyMatch(argument -> Set.of(
                    "-e", "--eval", "-p", "--print").contains(argument))
                || hasStdin && (arguments.isEmpty()
                    || arguments.stream().anyMatch(argument ->
                        argument.equals("--input-type")
                            || argument.startsWith("--input-type=")));
            case "python", "python3", "py" -> arguments.stream().anyMatch(argument ->
                    argument.equals("-c") || argument.equals("-"))
                || hasStdin && arguments.isEmpty();
            default -> false;
        };
        if (inline) {
            throw new CommandToolException("INLINE_INTERPRETER_DENIED",
                "execute_process cannot run inline interpreter source; use read_source/read_file "
                    + "for repository inspection or run_inline_probe during VERIFY");
        }
    }

    private static String normalizedExecutableName(String value) {
        String normalized = value.trim().replace('\\', '/');
        int separator = normalized.lastIndexOf('/');
        if (separator >= 0) {
            normalized = normalized.substring(separator + 1);
        }
        normalized = normalized.toLowerCase(Locale.ROOT);
        for (String suffix : List.of(".exe", ".cmd", ".bat")) {
            if (normalized.endsWith(suffix)) {
                return normalized.substring(0, normalized.length() - suffix.length());
            }
        }
        return normalized;
    }

    private void writeStandardInput(Process process, String stdin) throws IOException {
        byte[] bytes = stdin == null ? new byte[0] : stdin.getBytes(StandardCharsets.UTF_8);
        long maximum = Math.max(1L, (long) config.maxArgumentChars() * 4L);
        if (bytes.length > maximum) {
            throw new CommandToolException("STDIN_TOO_LARGE",
                "stdin exceeds the configured UTF-8 byte limit of " + maximum);
        }
        try (var output = process.getOutputStream()) {
            output.write(bytes);
        }
    }

    private long effectiveTimeout(Long requested) {
        if (requested == null) {
            return config.defaultTimeoutMs();
        }
        if (requested <= 0 || requested > config.maxTimeoutMs()) {
            throw new CommandToolException("INVALID_TIMEOUT",
                "timeoutMs must be positive and no greater than the configured maximum");
        }
        return requested;
    }

    private static long remainingTimeoutMillis(long started, long timeoutMs) {
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        return Math.max(1, timeoutMs - elapsed);
    }

    private static Thread collectorThread(BoundedOutputCollector collector, String stream,
                                          long pid) {
        Thread thread = new Thread(collector,
            "harness-execute-process-" + pid + "-" + stream);
        thread.setDaemon(true);
        return thread;
    }

    private static void awaitCollectors(Thread stdoutThread, Thread stderrThread,
                                        BoundedOutputCollector stdout,
                                        BoundedOutputCollector stderr,
                                        long graceMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(graceMs, 1_000));
        joinUntil(stdoutThread, deadline);
        joinUntil(stderrThread, deadline);
        if (stdoutThread.isAlive() || stderrThread.isAlive()) {
            stdout.close();
            stderr.close();
            joinUntil(stdoutThread, deadline + TimeUnit.MILLISECONDS.toNanos(500));
            joinUntil(stderrThread, deadline + TimeUnit.MILLISECONDS.toNanos(500));
        }
        if (stdoutThread.isAlive() || stderrThread.isAlive()) {
            throw new CommandToolException("OUTPUT_CAPTURE_STUCK",
                "Process output collectors did not terminate");
        }
    }

    private static void joinUntil(Thread thread, long deadlineNanos) throws InterruptedException {
        while (thread.isAlive()) {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            thread.join(Math.max(1, Math.min(100,
                TimeUnit.NANOSECONDS.toMillis(remaining))));
        }
    }

    private static void terminateProcessTree(Process process, long graceMs)
        throws InterruptedException {
        ProcessHandle parent = process.toHandle();
        List<ProcessHandle> descendants = descendantsDeepestFirst(parent);
        descendants.forEach(ExecuteProcessTool::destroyQuietly);
        waitForHandles(descendants, Math.max(50, graceMs / 3));
        List<ProcessHandle> remaining = new ArrayList<>(descendants);
        for (ProcessHandle discovered : descendantsDeepestFirst(parent)) {
            if (remaining.stream().noneMatch(existing -> existing.pid() == discovered.pid())) {
                remaining.add(discovered);
            }
        }
        remaining.forEach(ExecuteProcessTool::destroyForciblyQuietly);
        waitForHandles(remaining, Math.max(100, graceMs / 2));
        destroyQuietly(parent);
        waitForHandles(List.of(parent), Math.max(50, graceMs / 3));
        destroyForciblyQuietly(parent);
        List<ProcessHandle> all = new ArrayList<>(remaining);
        all.add(parent);
        waitForHandles(all, Math.max(100, graceMs));
        if (all.stream().anyMatch(ProcessHandle::isAlive)) {
            throw new CommandToolException("PROCESS_TERMINATION_FAILED",
                "Timed-out process tree could not be fully terminated");
        }
        process.waitFor(Math.max(1, graceMs), TimeUnit.MILLISECONDS);
    }

    private static List<ProcessHandle> descendantsDeepestFirst(ProcessHandle parent) {
        List<ProcessHandle> descendants = new ArrayList<>(parent.descendants().toList());
        Collections.reverse(descendants);
        return descendants;
    }

    private static void waitForHandles(List<ProcessHandle> handles, long timeoutMs)
        throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (handles.stream().anyMatch(ProcessHandle::isAlive) && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
    }

    private static void destroyQuietly(ProcessHandle handle) {
        try {
            if (handle.isAlive()) {
                handle.destroy();
            }
        } catch (RuntimeException ignored) {
            // The forced pass below is authoritative.
        }
    }

    private static void destroyForciblyQuietly(ProcessHandle handle) {
        try {
            if (handle.isAlive()) {
                handle.destroyForcibly();
            }
        } catch (RuntimeException ignored) {
            // Liveness is checked after every forced termination attempt.
        }
    }

    private static void terminateProcessTreeUninterruptibly(Process process, long graceMs) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    terminateProcessTree(process, graceMs);
                    return;
                } catch (InterruptedException error) {
                    interrupted = true;
                } catch (CommandToolException error) {
                    descendantsDeepestFirst(process.toHandle())
                        .forEach(ExecuteProcessTool::destroyForciblyQuietly);
                    destroyForciblyQuietly(process.toHandle());
                    return;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void closeAndJoinUninterruptibly(Thread stdoutThread, Thread stderrThread,
                                                     BoundedOutputCollector stdout,
                                                     BoundedOutputCollector stderr,
                                                     long graceMs) {
        if (stdout != null) {
            stdout.close();
        }
        if (stderr != null) {
            stderr.close();
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(graceMs);
        Thread[] threads = {stdoutThread, stderrThread};
        for (Thread thread : threads) {
            if (thread == null) {
                continue;
            }
            while (thread.isAlive() && System.nanoTime() < deadline) {
                try {
                    thread.join(25);
                } catch (InterruptedException ignored) {
                    // Original interrupt status is restored by the caller.
                }
            }
        }
    }

    private static int exitValue(Process process) {
        try {
            return process.exitValue();
        } catch (IllegalThreadStateException error) {
            return -1;
        }
    }

    private static long elapsedMillis(long started) {
        return Math.max(0, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
    }
}
