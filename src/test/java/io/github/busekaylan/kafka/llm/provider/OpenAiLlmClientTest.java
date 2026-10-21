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

class OpenAiLlmClientTest {

    private static final String SCHEMA = "{\"type\":\"object\",\"properties\":{\"category\":{\"type\":\"string\"}},"
            + "\"required\":[\"category\"],\"additionalProperties\":false}";
    private MockApi api;

    @BeforeEach
    void setUp() throws Exception {
        api = new MockApi();
        api.responseHeaders = Map.of("x-request-id", "req_openai");
        api.reply(200, completion("{\\\"category\\\":\\\"billing\\\"}", "null", "stop"));
    }

    @AfterEach
    void tearDown() {
        api.close();
    }

    private static String completion(final String content, final String refusal, final String finishReason) {
        final String contentJson = content == null ? "null" : "\"" + content + "\"";
        return """
                {"id":"chatcmpl-1","object":"chat.completion","created":1760000000,"model":"gpt-5.5",
                 "choices":[{"index":0,"finish_reason":"%s","logprobs":null,
                   "message":{"role":"assistant","content":%s,"refusal":%s}}],
                 "usage":{"prompt_tokens":31,"completion_tokens":9,"total_tokens":40}}
                """.formatted(finishReason, contentJson, refusal);
    }

    private LlmClient client(final String schema, final String effort) {
        return new OpenAiLlmClient(MockApi.settings(Provider.OPENAI, api.url(), "gpt-5.5", schema, effort));
    }

    @Test
    void sendsChatCompletionRequestAndParsesAnswer() {
        try (LlmClient llm = client(SCHEMA, "max")) {
            final LlmResult result = llm.complete("I was charged twice");
            assertEquals("{\"category\":\"billing\"}", result.text());
            assertEquals("gpt-5.5", result.model());
            assertEquals("stop", result.stopReason());
            assertEquals(31, result.inputTokens());
            assertEquals(9, result.outputTokens());
            assertEquals("req_openai", result.requestId());
            assertFalse(result.refused());
            assertFalse(result.truncated());

            final MockApi.Request request = api.last();
            assertEquals("/chat/completions", request.path());
            assertEquals("Bearer test-key", request.header("Authorization"));
            assertEquals("gpt-5.5", request.body().get("model").asText());
            assertEquals(1024, request.body().get("max_completion_tokens").asInt());
            assertEquals("system", request.body().at("/messages/0/role").asText());
            assertEquals("You are a triage assistant.", request.body().at("/messages/0/content").asText());
            assertEquals("<document>\nI was charged twice\n</document>\n\nClassify this ticket.", request.body().at("/messages/1/content").asText());
            assertEquals("xhigh", request.body().get("reasoning_effort").asText(), "max maps to OpenAI's xhigh");
            assertEquals("json_schema", request.body().at("/response_format/type").asText());
            assertTrue(request.body().at("/response_format/json_schema/strict").asBoolean());
            assertEquals("output", request.body().at("/response_format/json_schema/name").asText());
            assertFalse(request.body().at("/response_format/json_schema/schema/additionalProperties").asBoolean(true));
        }
    }

    @Test
    void omitsEffortAndSchemaWhenNotSet() {
        try (LlmClient llm = client(null, LlmSettings.EFFORT_NOT_SET)) {
            llm.complete("text");
            assertFalse(api.last().body().has("reasoning_effort"));
            assertFalse(api.last().body().has("response_format"));
        }
    }

    @Test
    void detectsRefusalAndTruncation() {
        try (LlmClient llm = client(SCHEMA, "low")) {
            api.reply(200, completion(null, "\"I can't help with that.\"", "stop"));
            final LlmResult refused = llm.complete("text");
            assertTrue(refused.refused());
            assertEquals("I can't help with that.", refused.refusalReason());

            api.reply(200, completion("", "null", "content_filter"));
            assertTrue(llm.complete("text").refused());

            api.reply(200, completion("{\\\"cat", "null", "length"));
            assertTrue(llm.complete("text").truncated());
        }
    }

    @Test
    void mapsErrors() {
        try (LlmClient llm = client(null, "low")) {
            api.reply(400, "{\"error\":{\"message\":\"bad model\",\"type\":\"invalid_request_error\",\"code\":null}}");
            final LlmException bad = assertThrows(LlmException.class, () -> llm.complete("text"));
            assertEquals(400, bad.status());
            assertFalse(bad.retryable());

            api.reply(429, "{\"error\":{\"message\":\"slow down\",\"type\":\"rate_limit_exceeded\",\"code\":null}}");
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
