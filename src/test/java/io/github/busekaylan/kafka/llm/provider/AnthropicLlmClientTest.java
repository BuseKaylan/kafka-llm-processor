package io.github.busekaylan.kafka.llm.provider;

import io.github.busekaylan.kafka.llm.provider.LlmClient.LlmException;
import io.github.busekaylan.kafka.llm.provider.LlmClient.LlmResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AnthropicLlmClientTest {

    private static final String SCHEMA = "{\"type\":\"object\",\"properties\":{\"category\":{\"type\":\"string\"}}}";
    private MockApi api;

    @BeforeEach
    void setUp() throws Exception {
        api = new MockApi();
        api.responseHeaders = Map.of("request-id", "req_anthropic");
        api.reply(200, """
                {"id":"msg_1","type":"message","role":"assistant","model":"claude-opus-5-5",
                 "content":[{"type":"text","text":"{\\"category\\":\\"billing\\"}"}],
                 "stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":30,"output_tokens":8}}
                """);
    }

    @AfterEach
    void tearDown() {
        api.close();
    }

    private LlmClient client(final String model, final String schema, final String effort) {
        return new AnthropicLlmClient(MockApi.settings(Provider.ANTHROPIC, api.url(), model, schema, effort));
    }

    @Test
    void sendsMessagesRequestAndParsesAnswer() {
        try (LlmClient llm = client("claude-opus-5-5", SCHEMA, "low")) {
            final LlmResult result = llm.complete("I was charged twice");
            assertEquals("{\"category\":\"billing\"}", result.text());
            assertEquals("claude-opus-5-5", result.model());
            assertEquals("end_turn", result.stopReason());
            assertEquals(30, result.inputTokens());
            assertEquals(8, result.outputTokens());
            assertEquals("req_anthropic", result.requestId());
            assertFalse(result.refused());
            assertFalse(result.truncated());

            final MockApi.Request request = api.last();
            assertEquals("/v1/messages", request.path());
            assertEquals("test-key", request.header("x-api-key"));
            assertEquals("You are a triage assistant.", request.body().get("system").asText());
            assertEquals("<document>\nI was charged twice\n</document>", request.body().at("/messages/0/content/0/text").asText());
            assertEquals("Classify this ticket.", request.body().at("/messages/0/content/1/text").asText());
            assertEquals(1024, request.body().get("max_tokens").asInt());
            assertEquals("low", request.body().at("/output_config/effort").asText());
            assertEquals("json_schema", request.body().at("/output_config/format/type").asText());
            assertEquals("default", request.body().get("fallbacks").asText());
            assertEquals(AnthropicLlmClient.FALLBACK_BETA, request.header("anthropic-beta"));
        }
    }

    @Test
    void omitsUnsupportedOptions() {
        try (LlmClient llm = client("claude-haiku-5-5", null, LlmSettings.EFFORT_NOT_SET)) {
            llm.complete("text");
            assertFalse(api.last().body().has("fallbacks"));
            assertFalse(api.last().body().has("output_config"));
            assertNull(api.last().header("anthropic-beta"));
        }
    }

    @Test
    void detectsRefusalAndTruncation() {
        api.reply(200, """
                {"id":"msg_2","type":"message","role":"assistant","model":"claude-opus-5-5","content":[],
                 "stop_reason":"refusal","stop_sequence":null,
                 "stop_details":{"type":"refusal","category":"cyber","explanation":"declined"},
                 "usage":{"input_tokens":5,"output_tokens":0}}
                """);
        try (LlmClient llm = client("claude-opus-5-5", null, "low")) {
            final LlmResult refused = llm.complete("text");
            assertTrue(refused.refused());
            assertEquals("cyber", refused.refusalReason());

            api.reply(200, """
                    {"id":"msg_3","type":"message","role":"assistant","model":"claude-opus-5-5",
                     "content":[{"type":"text","text":"{\\"cat"}],"stop_reason":"max_tokens","stop_sequence":null,
                     "usage":{"input_tokens":5,"output_tokens":1024}}
                    """);
            assertTrue(llm.complete("text").truncated());
        }
    }

    @Test
    void mapsErrors() {
        try (LlmClient llm = client("claude-opus-5-5", null, "low")) {
            api.reply(400, "{\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"bad model\"}}");
            final LlmException bad = assertThrows(LlmException.class, () -> llm.complete("text"));
            assertEquals(400, bad.status());
            assertFalse(bad.retryable());

            api.reply(429, "{\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"slow down\"}}");
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
