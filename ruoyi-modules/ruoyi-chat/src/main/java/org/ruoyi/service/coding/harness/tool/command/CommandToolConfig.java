package org.ruoyi.service.coding.harness.tool.command;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Resource and authorization limits for execute_process.
 *
 * <p>The executable allowlist is authorization metadata, not a sandbox. Allowed programs such as
 * npm, git, build tools, test runners, and language runtimes can execute repository-controlled
 * code. An outer policy, per-call approval, OS sandbox, and workspace lease remain mandatory.</p>
 */
public record CommandToolConfig(
    Set<String> executableAllowlist,
    Set<String> inheritedEnvironmentKeys,
    Map<String, String> additionalEnvironment,
    int maxConcurrentProcesses,
    long defaultTimeoutMs,
    long maxTimeoutMs,
    int maxOutputBytesPerStream,
    int maxArgvEntries,
    int maxArgumentChars,
    long terminationGraceMs
) {

    public static final Set<String> DEFAULT_EXECUTABLE_ALLOWLIST = Set.of(
        "git", "git.exe", "java", "java.exe", "javac", "javac.exe",
        "mvn", "mvn.cmd", "mvn.exe", "gradle", "gradle.bat",
        "node", "node.exe", "npm", "npm.cmd", "npx", "npx.cmd",
        "pnpm", "pnpm.cmd", "pnpm.exe",
        "python", "python.exe", "python3", "python3.exe", "rg", "rg.exe"
    );

    public static final Set<String> DEFAULT_INHERITED_ENVIRONMENT = Set.of(
        "PATH", "JAVA_HOME", "MAVEN_HOME", "M2_HOME", "GRADLE_HOME", "NODE_HOME",
        "PYTHONHOME", "HOME", "USERPROFILE", "TMP", "TEMP", "SystemRoot", "WINDIR",
        "PATHEXT", "LANG", "LC_ALL"
    );

    public static final CommandToolConfig DEFAULT = new CommandToolConfig(
        DEFAULT_EXECUTABLE_ALLOWLIST,
        DEFAULT_INHERITED_ENVIRONMENT,
        Map.of(),
        2,
        30_000,
        120_000,
        256 * 1024,
        256,
        16 * 1024,
        2_000
    );

    public CommandToolConfig {
        if (executableAllowlist == null || executableAllowlist.isEmpty()) {
            throw new IllegalArgumentException("Executable allowlist must not be empty");
        }
        LinkedHashSet<String> executables = new LinkedHashSet<>();
        for (String executable : executableAllowlist) {
            executables.add(CommandValidation.requireText(executable, "allowlisted executable", 4_096));
        }
        executableAllowlist = Set.copyOf(executables);

        inheritedEnvironmentKeys = inheritedEnvironmentKeys == null
            ? Set.of() : Set.copyOf(inheritedEnvironmentKeys);
        for (String key : inheritedEnvironmentKeys) {
            ControlledEnvironment.validateKeyForInheritance(key);
        }
        additionalEnvironment = additionalEnvironment == null
            ? Map.of() : Map.copyOf(additionalEnvironment);
        additionalEnvironment.forEach(ControlledEnvironment::validateAdditionalEntry);

        if (maxConcurrentProcesses <= 0 || defaultTimeoutMs <= 0 || maxTimeoutMs <= 0
            || defaultTimeoutMs > maxTimeoutMs || maxOutputBytesPerStream <= 0
            || maxArgvEntries <= 0 || maxArgumentChars <= 0 || terminationGraceMs <= 0) {
            throw new IllegalArgumentException("Command tool limits must be positive and ordered");
        }
    }
}
