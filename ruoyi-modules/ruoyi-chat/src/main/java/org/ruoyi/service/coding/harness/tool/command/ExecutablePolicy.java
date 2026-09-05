package org.ruoyi.service.coding.harness.tool.command;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Resolves an authorized executable without searching the workspace cwd. */
final class ExecutablePolicy {

    private static final Set<String> SHELL_INTERPRETERS = Set.of(
        "cmd", "command", "powershell", "pwsh", "sh", "bash", "dash", "zsh",
        "fish", "csh", "ksh", "wsl"
    );

    private final CommandToolConfig config;
    private final Map<String, String> environment;
    private final boolean windows;

    ExecutablePolicy(CommandToolConfig config, Map<String, String> environment) {
        this.config = config;
        this.environment = environment;
        this.windows = System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
    }

    Path authorize(String requested) {
        String executable = CommandValidation.requireText(requested, "executable", 4_096);
        final Path supplied;
        try {
            supplied = Path.of(executable);
        } catch (InvalidPathException error) {
            throw new CommandToolException("INVALID_EXECUTABLE", "Executable path is invalid", error);
        }
        if (isShell(supplied.getFileName() == null ? executable : supplied.getFileName().toString())) {
            throw new CommandToolException("SHELL_INTERPRETER_DENIED",
                "Shell interpreters are never valid execute_process executables");
        }
        if (supplied.isAbsolute()) {
            return authorizeAbsolute(supplied);
        }
        if (supplied.getNameCount() != 1 || executable.contains("/") || executable.contains("\\")) {
            throw new CommandToolException("EXECUTABLE_NOT_ALLOWLISTED",
                "Executable must be an allowlisted name or allowlisted absolute path");
        }
        if (config.executableAllowlist().stream().noneMatch(allowed ->
            !isPathLike(allowed) && namesEqual(allowed, executable))) {
            throw new CommandToolException("EXECUTABLE_NOT_ALLOWLISTED",
                "Executable is not authorized by the command allowlist");
        }
        Path resolved = findOnControlledPath(executable);
        if (resolved == null) {
            throw new CommandToolException("EXECUTABLE_NOT_FOUND",
                "Allowlisted executable was not found on the controlled PATH");
        }
        return resolved;
    }

    private Path authorizeAbsolute(Path supplied) {
        Path normalized = supplied.toAbsolutePath().normalize();
        boolean allowed = config.executableAllowlist().stream()
            .filter(ExecutablePolicy::isPathLike)
            .map(ExecutablePolicy::normalizedPath)
            .anyMatch(path -> pathsEqual(path, normalized));
        if (!allowed) {
            throw new CommandToolException("EXECUTABLE_NOT_ALLOWLISTED",
                "Absolute executable is not authorized by the command allowlist");
        }
        return realExecutable(normalized);
    }

    private Path findOnControlledPath(String executable) {
        String path = ControlledEnvironment.path(environment);
        if (path == null || path.isBlank()) {
            return null;
        }
        for (String entry : path.split(java.util.regex.Pattern.quote(File.pathSeparator), -1)) {
            String directoryText = unquote(entry.strip());
            if (directoryText.isEmpty()) {
                continue;
            }
            Path directory;
            try {
                directory = Path.of(directoryText).toAbsolutePath().normalize();
            } catch (InvalidPathException ignored) {
                continue;
            }
            for (String candidateName : candidateNames(executable)) {
                Path candidate = directory.resolve(candidateName).normalize();
                if (Files.isRegularFile(candidate)
                    && (windows || Files.isExecutable(candidate))) {
                    return realExecutable(candidate);
                }
            }
        }
        return null;
    }

    private List<String> candidateNames(String executable) {
        List<String> names = new ArrayList<>();
        if (!windows || executable.lastIndexOf('.') >= 0) {
            names.add(executable);
            return names;
        }
        String pathExt = environment.entrySet().stream()
            .filter(entry -> entry.getKey().equalsIgnoreCase("PATHEXT"))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElse(".EXE;.COM;.BAT;.CMD");
        for (String extension : pathExt.split(";")) {
            if (!extension.isBlank()) {
                names.add(executable + extension.toLowerCase(Locale.ROOT));
                names.add(executable + extension.toUpperCase(Locale.ROOT));
            }
        }
        // A Windows PATH can contain both a Unix launcher without an extension and the native
        // .cmd/.exe launcher (Maven distributions commonly do). CreateProcess cannot start the
        // Unix script, so PATHEXT candidates must win before the extensionless fallback.
        names.add(executable);
        return names;
    }

    private Path realExecutable(Path executable) {
        try {
            Path real = executable.toRealPath();
            if (!Files.isRegularFile(real) || (!windows && !Files.isExecutable(real))) {
                throw new CommandToolException("EXECUTABLE_NOT_FOUND",
                    "Authorized executable is not a runnable regular file");
            }
            return real;
        } catch (IOException error) {
            throw new CommandToolException("EXECUTABLE_NOT_FOUND",
                "Authorized executable cannot be resolved", error);
        }
    }

    private boolean namesEqual(String first, String second) {
        return windows ? first.equalsIgnoreCase(second) : first.equals(second);
    }

    private boolean pathsEqual(Path first, Path second) {
        return windows ? first.toString().equalsIgnoreCase(second.toString()) : first.equals(second);
    }

    private static boolean isShell(String name) {
        String normalized = name.toLowerCase(Locale.ROOT);
        for (String extension : List.of(".exe", ".cmd", ".bat", ".com")) {
            if (normalized.endsWith(extension)) {
                normalized = normalized.substring(0, normalized.length() - extension.length());
                break;
            }
        }
        return SHELL_INTERPRETERS.contains(normalized);
    }

    private static boolean isPathLike(String value) {
        try {
            return Path.of(value).isAbsolute() || value.contains("/") || value.contains("\\");
        } catch (InvalidPathException error) {
            return true;
        }
    }

    private static Path normalizedPath(String value) {
        try {
            return Path.of(value).toAbsolutePath().normalize();
        } catch (InvalidPathException error) {
            throw new IllegalArgumentException("Invalid allowlisted executable path", error);
        }
    }

    private static String unquote(String value) {
        if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }
}
