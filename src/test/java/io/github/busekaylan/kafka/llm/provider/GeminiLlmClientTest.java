package io.github.busekaylan.kafka.llm.provider;

import io.github.busekaylan.kafka.llm.provider.LlmClient.LlmException;
import io.github.busekaylan.kafka.llm.provider.LlmClient.LlmResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GeminiLlmClientTest {

    private static final String SCHEMA = "{\"type\":\"object\",\"properties\":{\"category\":{\"type\":\"string\"}}}";
    private MockApi api;

    @BeforeEach
    void setUp() throws Exception {
        api = new MockApi();
        api.reply(200, response("{\\\"category\\\":\\\"billing\\\"}", "STOP"));
    }

    @AfterEach
    void tearDown() {
        api.close();
    }

    private static String response(final String text, final String finishReason) {
        return """
                {"candidates":[{"content":{"role":"model","parts":[
                    {"text":"thinking about it","thought":true},{"text":"%s"}]},"finishReason":"%s","index":0}],
                 "usageMetadata":{"promptTokenCount":33,"candidatesTokenCount":10,"totalTokenCount":43},
                 "modelVersion":"gemini-flash-latest","responseId":"resp_gemini"}
                """.formatted(text, finishReason);
    }

    private LlmClient client(final String schema, final String effort) {
        return new GeminiLlmClient(MockApi.settings(Provider.GEMINI, api.url(), "gemini-flash-latest", schema, effort));
    }

    @Test
    void sendsGenerateContentRequestAndParsesAnswer() {
        try (LlmClient llm = client(SCHEMA, "xhigh")) {
            final LlmResult result = llm.complete("I was charged twice");
            assertEquals("{\"category\":\"billing\"}", result.text(), "thought parts are skipped");
            assertEquals("gemini-flash-latest", result.model());
            assertEquals("STOP", result.stopReason());
            assertEquals(33, result.inputTokens());
            assertEquals(10, result.outputTokens());
            assertEquals("resp_gemini", result.requestId());
            assertFalse(result.refused());
            assertFalse(result.truncated());

            final MockApi.Request request = api.last();
            assertTrue(request.path().endsWith("/models/gemini-flash-latest:generateContent"), request.path());
            assertEquals("test-key", request.header("x-goog-api-key"));
            assertEquals("<document>\nI was charged twice\n</document>\n\nClassify this ticket.",
                    request.body().at("/contents/0/parts/0/text").asText());
            assertEquals("You are a triage assistant.", request.body().at("/systemInstruction/parts/0/text").asText());
            assertEquals(1024, request.body().at("/generationConfig/maxOutputTokens").asInt());
            assertEquals("application/json", request.body().at("/generationConfig/responseMimeType").asText());
            assertEquals("object", request.body().at("/generationConfig/responseJsonSchema/type").asText());
            assertEquals("HIGH", request.body().at("/generationConfig/thinkingConfig/thinkingLevel").asText(), "xhigh maps to Gemini's high");
        }
    }

    @Test
    void omitsEffortAndSchemaWhenNotSet() {
        try (LlmClient llm = client(null, LlmSettings.EFFORT_NOT_SET)) {
            llm.complete("text");
            final var config = api.last().body().at("/generationConfig");
            assertFalse(config.has("thinkingConfig"));
            assertFalse(config.has("responseJsonSchema"));
        }
    }

    @Test
    void detectsBlockedAndTruncatedResponses() {
        try (LlmClient llm = client(null, "low")) {
            api.reply(200, response("", "SAFETY"));
            final LlmResult safety = llm.complete("text");
            assertTrue(safety.refused());
            assertEquals("SAFETY", safety.refusalReason());
            assertEquals("", safety.text());

            api.reply(200, """
                    {"promptFeedback":{"blockReason":"PROHIBITED_CONTENT"},
                     "usageMetadata":{"promptTokenCount":12,"totalTokenCount":12},"modelVersion":"gemini-flash-latest"}
                    """);
            final LlmResult blockedPrompt = llm.complete("text");
            assertTrue(blockedPrompt.refused());
            assertEquals("PROHIBITED_CONTENT", blockedPrompt.refusalReason());

            api.reply(200, response("{\\\"cat", "MAX_TOKENS"));
            assertTrue(llm.complete("text").truncated());
        }
    }

    @Test
    void mapsErrors() {
        try (LlmClient llm = client(null, "low")) {
            api.reply(400, "{\"error\":{\"code\":400,\"message\":\"bad model\",\"status\":\"INVALID_ARGUMENT\"}}");
            final LlmException bad = assertThrows(LlmException.class, () -> llm.complete("text"));
            assertEquals(400, bad.status());
            assertFalse(bad.retryable());

            api.reply(429, "{\"error\":{\"code\":429,\"message\":\"slow down\",\"status\":\"RESOURCE_EXHAUSTED\"}}");
            final LlmException limited = assertThrows(LlmException.class, () -> llm.complete("text"));
            assertEquals(429, limited.status());
            assertTrue(limited.retryable());

            api.stop();
            final LlmException network = assertThrows(LlmException.class, () -> llm.complete("text"));
            assertNull(network.status());
            assertTrue(network.retryable());
        }
    }
}
