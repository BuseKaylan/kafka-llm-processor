package io.github.busekaylan.kafka.llm.provider;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Map;

/**
 * Provider-independent request settings.
 *
 * @param effort one of {@code low, medium, high, xhigh, max} or {@code not-set}; each client maps it to its
 *               provider's closest setting
 */
public record LlmSettings(
        Provider provider,
        String apiKey,
        String baseUrl,
        String model,
        String systemPrompt,
        String prompt,
        String outputSchema,
        String effort,
        long maxTokens,
        Duration timeout,
        int maxRetries,
        boolean anthropicRefusalFallback) {

    public static final String EFFORT_NOT_SET = "not-set";

    boolean hasEffort() {
        return !EFFORT_NOT_SET.equals(effort);
    }

    /** The output schema as a map, or null when none is set. */
    @SuppressWarnings("unchecked")
    Map<String, Object> schemaMap() {
        if (outputSchema == null) {
            return null;
        }
        try {
            return new ObjectMapper().readValue(outputSchema, Map.class);
        } catch (final Exception e) {
            throw new IllegalArgumentException("Output JSON schema is not valid JSON", e);
        }
    }
}
