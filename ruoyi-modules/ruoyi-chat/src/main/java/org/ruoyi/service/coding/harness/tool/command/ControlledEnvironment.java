package org.ruoyi.service.coding.harness.tool.command;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

/** Builds a minimal environment instead of forwarding the worker process environment wholesale. */
final class ControlledEnvironment {

    private static final Pattern KEY = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,127}");
    private static final int MAX_VALUE_CHARS = 32 * 1024;

    private ControlledEnvironment() {
    }

    static Map<String, String> build(CommandToolConfig config) {
        boolean windows = isWindows();
        Map<String, String> result = windows
            ? new TreeMap<>(String.CASE_INSENSITIVE_ORDER) : new LinkedHashMap<>();
        Map<String, String> parent = System.getenv();
        for (String requested : config.inheritedEnvironmentKeys()) {
            Map.Entry<String, String> inherited = find(parent, requested, windows);
            if (inherited != null) {
                result.put(inherited.getKey(), validateValue(inherited.getValue(), inherited.getKey()));
            }
        }
        config.additionalEnvironment().forEach((key, value) ->
            result.put(key, validateValue(value, key)));
        return Map.copyOf(result);
    }

    static void validateKeyForInheritance(String key) {
        validateKey(key);
        if (isSensitive(key)) {
            throw new IllegalArgumentException(
                "Sensitive environment variables cannot be inherited: " + key);
        }
    }

    static void validateAdditionalEntry(String key, String value) {
        validateKey(key);
        if (isSensitive(key)) {
            throw new IllegalArgumentException(
                "Sensitive environment variables cannot be injected: " + key);
        }
        validateValue(value, key);
    }

    static String path(Map<String, String> environment) {
        Map.Entry<String, String> entry = find(environment, "PATH", isWindows());
        return entry == null ? null : entry.getValue();
    }

    private static void validateKey(String key) {
        if (key == null || !KEY.matcher(key).matches()) {
            throw new IllegalArgumentException("Invalid environment variable name");
        }
    }

    private static String validateValue(String value, String key) {
        if (value == null || value.length() > MAX_VALUE_CHARS) {
            throw new IllegalArgumentException("Invalid environment value for " + key);
        }
        for (int i = 0; i < value.length(); i++) {
            if (Character.isISOControl(value.charAt(i))) {
                throw new IllegalArgumentException(
                    "Environment value contains NUL or control characters: " + key);
            }
        }
        return value;
    }

    private static boolean isSensitive(String key) {
        String upper = key.toUpperCase(Locale.ROOT);
        if (upper.contains("TOKEN") || upper.contains("SECRET")
            || upper.contains("PASSWORD") || upper.contains("CREDENTIAL")
            || upper.contains("PRIVATE_KEY") || upper.contains("API_KEY")
            || upper.contains("ACCESS_KEY") || upper.contains("AUTH")
            || upper.contains("JWT") || upper.contains("COOKIE")
            || upper.contains("BEARER")) {
            return true;
        }
        Set<String> prefixes = Set.of(
            "AWS_", "AZURE_", "GITHUB_", "GITLAB_", "OPENAI_", "ANTHROPIC_",
            "GOOGLE_", "GCP_", "SSH_", "KUBE", "DOCKER_AUTH"
        );
        return prefixes.stream().anyMatch(upper::startsWith);
    }

    private static Map.Entry<String, String> find(Map<String, String> values, String requested,
                                                   boolean ignoreCase) {
        for (Map.Entry<String, String> entry : values.entrySet()) {
            if (ignoreCase ? entry.getKey().equalsIgnoreCase(requested)
                : entry.getKey().equals(requested)) {
                return entry;
            }
        }
        return null;
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
    }
}
