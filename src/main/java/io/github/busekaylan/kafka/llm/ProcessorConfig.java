package io.github.busekaylan.kafka.llm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.busekaylan.kafka.llm.provider.LlmSettings;
import io.github.busekaylan.kafka.llm.provider.Provider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Application settings, read from environment variables.
 *
 * <p>Every variable that starts with {@code KAFKA_} is passed to Kafka Streams as a property:
 * {@code KAFKA_BOOTSTRAP_SERVERS} becomes {@code bootstrap.servers},
 * {@code KAFKA_SECURITY_PROTOCOL} becomes {@code security.protocol}, and so on.
 */
public record ProcessorConfig(
        String inputTopic,
        String outputTopic,
        String refusedTopic,
        String dlqTopic,
        LlmSettings llm,
        int maxInputBytes,
        RetryableErrorAction retryableErrorAction,
        Properties streamsProperties) {

    /** What to do when the LLM API is unreachable after all retries (rate limit, overload, network). */
    public enum RetryableErrorAction {
        /** Send the record to the dead letter topic and keep going. */
        DLQ,
        /** Stop the application without committing, so the record is processed again on restart. */
        STOP
    }

    static final Set<String> EFFORTS = Set.of("low", "medium", "high", "xhigh", "max", LlmSettings.EFFORT_NOT_SET);
    private static final String KAFKA_PREFIX = "KAFKA_";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static ProcessorConfig fromEnv(final Map<String, String> env) {
        final List<String> problems = new ArrayList<>();

        final String inputTopic = required(env, "INPUT_TOPIC", problems);
        final String prompt = required(env, "LLM_PROMPT", problems);
        final String base = inputTopic == null ? "input" : inputTopic;

        Provider provider = null;
        final String providerValue = get(env, "LLM_PROVIDER", null);
        if (providerValue == null) {
            problems.add("LLM_PROVIDER is required: " + providerNames());
        } else {
            try {
                provider = Provider.parse(providerValue);
            } catch (final IllegalArgumentException e) {
                problems.add("LLM_PROVIDER must be one of " + providerNames() + ", was '" + providerValue + "'");
            }
        }

        String apiKey = get(env, "LLM_API_KEY", null);
        String model = get(env, "LLM_MODEL", null);
        if (provider != null) {
            if (apiKey == null) {
                apiKey = get(env, provider.apiKeyVariable(), null);
            }
            if (apiKey == null) {
                problems.add("LLM_API_KEY (or " + provider.apiKeyVariable() + ") is required");
            }
            if (model == null) {
                model = provider.defaultModel();
            }
            if (model == null) {
                problems.add("LLM_MODEL is required for provider " + provider.id());
            }
        }

        final String effort = get(env, "LLM_EFFORT", LlmSettings.EFFORT_NOT_SET).toLowerCase(Locale.ROOT);
        if (!EFFORTS.contains(effort)) {
            problems.add("LLM_EFFORT must be one of " + EFFORTS + ", was '" + effort + "'");
        }

        final String schema = readSchema(env, problems);
        final long maxTokens = number(env, "LLM_MAX_TOKENS", 16_000, 1, 128_000, problems);
        final long timeoutSeconds = number(env, "LLM_TIMEOUT_SECONDS", 300, 1, 3_600, problems);
        final long maxRetries = number(env, "LLM_MAX_RETRIES", 2, 0, 10, problems);
        final long maxInputBytes = number(env, "MAX_INPUT_BYTES", 2 * 1024 * 1024, 1, Integer.MAX_VALUE, problems);

        RetryableErrorAction action = RetryableErrorAction.DLQ;
        final String actionValue = get(env, "RETRYABLE_ERROR_ACTION", "dlq");
        try {
            action = RetryableErrorAction.valueOf(actionValue.toUpperCase(Locale.ROOT));
        } catch (final IllegalArgumentException e) {
            problems.add("RETRYABLE_ERROR_ACTION must be 'dlq' or 'stop', was '" + actionValue + "'");
        }

        final Properties streams = streamsProperties(env);
        if (!streams.containsKey("bootstrap.servers")) {
            problems.add("KAFKA_BOOTSTRAP_SERVERS is required");
        }

        if (!problems.isEmpty()) {
            throw new IllegalArgumentException("Invalid configuration:\n  - " + String.join("\n  - ", problems));
        }

        final LlmSettings llm = new LlmSettings(
                provider,
                apiKey,
                get(env, "LLM_BASE_URL", null),
                model,
                get(env, "LLM_SYSTEM_PROMPT", null),
                prompt,
                schema,
                effort,
                maxTokens,
                Duration.ofSeconds(timeoutSeconds),
                (int) maxRetries,
                Boolean.parseBoolean(get(env, "ANTHROPIC_REFUSAL_FALLBACK", "true")));

        return new ProcessorConfig(
                inputTopic,
                get(env, "OUTPUT_TOPIC", base + ".llm"),
                get(env, "REFUSED_TOPIC", base + ".refused"),
                get(env, "DLQ_TOPIC", base + ".dlq"),
                llm,
                (int) maxInputBytes,
                action,
                streams);
    }

    private static String providerNames() {
        return Arrays.stream(Provider.values()).map(Provider::id).toList().toString();
    }

    private static Properties streamsProperties(final Map<String, String> env) {
        final Properties props = new Properties();
        props.put("application.id", "kafka-llm-processor");
        // LLM calls take seconds, so poll few records at a time and allow long gaps between polls.
        // Kafka's defaults (500 records, 5 minutes) would trigger rebalances under load.
        props.put("max.poll.records", "10");
        props.put("max.poll.interval.ms", "900000");
        env.forEach((name, value) -> {
            if (name.startsWith(KAFKA_PREFIX) && value != null && !value.isBlank()) {
                props.put(name.substring(KAFKA_PREFIX.length()).toLowerCase(Locale.ROOT).replace('_', '.'), value);
            }
        });
        return props;
    }

    private static String readSchema(final Map<String, String> env, final List<String> problems) {
        String schema = get(env, "LLM_OUTPUT_SCHEMA", null);
        final String file = get(env, "LLM_OUTPUT_SCHEMA_FILE", null);
        if (schema == null && file != null) {
            try {
                schema = Files.readString(Path.of(file));
            } catch (final IOException e) {
                problems.add("LLM_OUTPUT_SCHEMA_FILE could not be read: " + e.getMessage());
                return null;
            }
        }
        if (schema == null) {
            return null;
        }
        try {
            final JsonNode node = MAPPER.readTree(schema);
            if (node == null || !node.isObject()) {
                problems.add("Output JSON schema must be a JSON object");
            }
        } catch (final IOException e) {
            problems.add("Output JSON schema is not valid JSON: " + e.getMessage());
        }
        return schema;
    }

    private static String get(final Map<String, String> env, final String name, final String fallback) {
        final String value = env.get(name);
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    private static String required(final Map<String, String> env, final String name, final List<String> problems) {
        final String value = get(env, name, null);
        if (value == null) {
            problems.add(name + " is required");
        }
        return value;
    }

    private static long number(final Map<String, String> env, final String name, final long fallback,
                               final long min, final long max, final List<String> problems) {
        final String value = get(env, name, null);
        if (value == null) {
            return fallback;
        }
        try {
            final long parsed = Long.parseLong(value);
            if (parsed < min || parsed > max) {
                problems.add(name + " must be between " + min + " and " + max + ", was " + parsed);
            }
            return parsed;
        } catch (final NumberFormatException e) {
            problems.add(name + " must be a number, was '" + value + "'");
            return fallback;
        }
    }

    @Override
    public String toString() {
        // Never print the API key.
        return "ProcessorConfig[input=%s, output=%s, refused=%s, dlq=%s, provider=%s, model=%s, effort=%s, schema=%s, retryableErrorAction=%s]"
                .formatted(inputTopic, outputTopic, refusedTopic, dlqTopic, llm.provider().id(), llm.model(), llm.effort(),
                        llm.outputSchema() != null, retryableErrorAction);
    }
}
