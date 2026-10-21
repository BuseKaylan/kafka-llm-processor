package io.github.busekaylan.kafka.llm;

import io.github.busekaylan.kafka.llm.provider.LlmSettings;
import io.github.busekaylan.kafka.llm.provider.Provider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcessorConfigTest {

    private static Map<String, String> minimal(final String provider) {
        return new HashMap<>(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "broker:9092",
                "INPUT_TOPIC", "tickets",
                "LLM_PROVIDER", provider,
                "LLM_API_KEY", "secret-key",
                "LLM_PROMPT", "Classify this ticket."));
    }

    @Test
    void appliesDefaults() {
        final ProcessorConfig config = ProcessorConfig.fromEnv(minimal("anthropic"));
        final LlmSettings llm = config.llm();
        assertEquals("tickets.llm", config.outputTopic());
        assertEquals("tickets.refused", config.refusedTopic());
        assertEquals("tickets.dlq", config.dlqTopic());
        assertEquals(Provider.ANTHROPIC, llm.provider());
        assertEquals("claude-opus-5-5", llm.model());
        assertEquals(LlmSettings.EFFORT_NOT_SET, llm.effort());
        assertEquals(16_000, llm.maxTokens());
        assertTrue(llm.anthropicRefusalFallback());
        assertNull(llm.outputSchema());
        assertEquals(ProcessorConfig.RetryableErrorAction.DLQ, config.retryableErrorAction());
        assertEquals("kafka-llm-processor", config.streamsProperties().get("application.id"));
        assertEquals("10", config.streamsProperties().get("max.poll.records"));
        assertEquals("900000", config.streamsProperties().get("max.poll.interval.ms"));
    }

    @Test
    void requiresModelForOpenAiAndGemini() {
        for (final String provider : new String[]{"openai", "gemini"}) {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> ProcessorConfig.fromEnv(minimal(provider)));
            assertTrue(e.getMessage().contains("LLM_MODEL is required for provider " + provider), e.getMessage());
        }
        final Map<String, String> env = minimal("GEMINI");
        env.put("LLM_MODEL", "gemini-flash-latest");
        assertEquals(Provider.GEMINI, ProcessorConfig.fromEnv(env).llm().provider());
    }

    @Test
    void acceptsProviderSpecificApiKeyVariable() {
        final Map<String, String> env = minimal("openai");
        env.remove("LLM_API_KEY");
        env.put("LLM_MODEL", "gpt-5.5");
        env.put("OPENAI_API_KEY", "openai-key");
        assertEquals("openai-key", ProcessorConfig.fromEnv(env).llm().apiKey());

        env.put("LLM_API_KEY", "generic-key");
        assertEquals("generic-key", ProcessorConfig.fromEnv(env).llm().apiKey(), "LLM_API_KEY wins");
    }

    @Test
    void passesKafkaVariablesThrough() {
        final Map<String, String> env = minimal("anthropic");
        env.put("KAFKA_SECURITY_PROTOCOL", "SASL_SSL");
        env.put("KAFKA_NUM_STREAM_THREADS", "4");
        env.put("KAFKA_MAX_POLL_RECORDS", "25");
        env.put("KAFKA_APPLICATION_ID", "ticket-triage");
        final var props = ProcessorConfig.fromEnv(env).streamsProperties();
        assertEquals("broker:9092", props.get("bootstrap.servers"));
        assertEquals("SASL_SSL", props.get("security.protocol"));
        assertEquals("4", props.get("num.stream.threads"));
        assertEquals("25", props.get("max.poll.records"));
        assertEquals("ticket-triage", props.get("application.id"));
    }

    @Test
    void reportsAllProblemsAtOnce() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> ProcessorConfig.fromEnv(Map.of(
                "LLM_PROVIDER", "mistral",
                "LLM_EFFORT", "extreme",
                "LLM_MAX_TOKENS", "0",
                "RETRYABLE_ERROR_ACTION", "ignore",
                "LLM_OUTPUT_SCHEMA", "[1,2]")));
        final String message = e.getMessage();
        for (final String expected : new String[]{"INPUT_TOPIC is required", "LLM_PROMPT is required",
                "LLM_PROVIDER must be one of [anthropic, openai, gemini]", "KAFKA_BOOTSTRAP_SERVERS is required",
                "LLM_EFFORT must be one of", "LLM_MAX_TOKENS must be between", "RETRYABLE_ERROR_ACTION must be",
                "must be a JSON object"}) {
            assertTrue(message.contains(expected), () -> "missing '" + expected + "' in:\n" + message);
        }
    }

    @Test
    void readsSchemaFromFile(@TempDir final Path dir) throws Exception {
        final Path schema = dir.resolve("schema.json");
        Files.writeString(schema, "{\"type\":\"object\"}");
        final Map<String, String> env = minimal("anthropic");
        env.put("LLM_OUTPUT_SCHEMA_FILE", schema.toString());
        assertEquals("{\"type\":\"object\"}", ProcessorConfig.fromEnv(env).llm().outputSchema());
    }

    @Test
    void toStringHidesApiKey() {
        assertFalse(ProcessorConfig.fromEnv(minimal("anthropic")).toString().contains("secret-key"));
    }
}
