package io.github.busekaylan.kafka.llm;

import io.github.busekaylan.kafka.llm.provider.LlmClient;
import io.github.busekaylan.kafka.llm.provider.LlmClient.LlmException;
import io.github.busekaylan.kafka.llm.provider.LlmClient.LlmResult;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LlmProcessorTest {

    private TopologyTestDriver driver;
    private TestInputTopic<byte[], byte[]> input;
    private TestOutputTopic<byte[], byte[]> output;
    private TestOutputTopic<byte[], byte[]> refused;
    private TestOutputTopic<byte[], byte[]> dlq;
    private final List<String> sent = new ArrayList<>();

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
    }

    private void start(final Function<String, LlmResult> llm, final Map<String, String> extraEnv) {
        final Map<String, String> env = new HashMap<>(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "dummy:9092",
                "INPUT_TOPIC", "tickets",
                "LLM_PROVIDER", "openai",
                "LLM_MODEL", "gpt-5.5",
                "LLM_API_KEY", "test-key",
                "LLM_PROMPT", "Classify this ticket."));
        env.putAll(extraEnv);
        final ProcessorConfig config = ProcessorConfig.fromEnv(env);
        final LlmClient client = content -> {
            sent.add(content);
            return llm.apply(content);
        };
        driver = new TopologyTestDriver(TopologyFactory.build(config, client), config.streamsProperties());
        input = driver.createInputTopic("tickets", new ByteArraySerializer(), new ByteArraySerializer());
        output = driver.createOutputTopic("tickets.llm", new ByteArrayDeserializer(), new ByteArrayDeserializer());
        refused = driver.createOutputTopic("tickets.refused", new ByteArrayDeserializer(), new ByteArrayDeserializer());
        dlq = driver.createOutputTopic("tickets.dlq", new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    private void start(final Function<String, LlmResult> llm) {
        start(llm, Map.of());
    }

    private static LlmResult answer(final String text) {
        return new LlmResult(text, "gpt-5.5", "stop", 42, 7, "req_1", false, null, false);
    }

    private static byte[] bytes(final String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String header(final Headers headers, final String key) {
        final var header = headers.lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    @Test
    void writesAnswerToOutputTopicAndKeepsKeyAndHeaders() {
        start(text -> answer("{\"category\":\"billing\"}"));
        final Headers in = new RecordHeaders().add("trace-id", bytes("abc")).add("llm.model", bytes("stale"));
        input.pipeInput(new TestRecord<>(bytes("ticket-1"), bytes("I was charged twice"), in));

        final TestRecord<byte[], byte[]> out = output.readRecord();
        assertArrayEquals(bytes("ticket-1"), out.key());
        assertEquals("{\"category\":\"billing\"}", new String(out.value(), StandardCharsets.UTF_8));
        assertEquals("abc", header(out.headers(), "trace-id"));
        assertEquals("openai", header(out.headers(), LlmProcessor.H_PROVIDER));
        assertEquals("gpt-5.5", header(out.headers(), LlmProcessor.H_MODEL));
        assertEquals(1, out.headers().toArray().length - out.headers().remove(LlmProcessor.H_MODEL).toArray().length,
                "stale llm.model header must be replaced, not duplicated");
        assertEquals("stop", header(out.headers(), LlmProcessor.H_STOP_REASON));
        assertEquals("42", header(out.headers(), LlmProcessor.H_INPUT_TOKENS));
        assertEquals("7", header(out.headers(), LlmProcessor.H_OUTPUT_TOKENS));
        assertEquals("req_1", header(out.headers(), LlmProcessor.H_REQUEST_ID));
        assertEquals("text/plain; charset=utf-8", header(out.headers(), LlmProcessor.H_CONTENT_TYPE));
        assertEquals(List.of("I was charged twice"), sent);
        assertTrue(refused.isEmpty());
        assertTrue(dlq.isEmpty());
    }

    @Test
    void marksJsonOutputWhenSchemaIsSet() {
        start(text -> answer("{\"category\":\"billing\"}"), Map.of("LLM_OUTPUT_SCHEMA", "{\"type\":\"object\"}"));
        input.pipeInput(bytes("k"), bytes("text"));
        assertEquals("application/json", header(output.readRecord().headers(), LlmProcessor.H_CONTENT_TYPE));
    }

    @Test
    void refusalGoesToRefusedTopicWithOriginalValue() {
        start(text -> new LlmResult("", "gpt-5.5", "stop", 5, 0, "req_2", true, "I can't help with that.", false));
        input.pipeInput(bytes("k"), bytes("original"));

        final TestRecord<byte[], byte[]> out = refused.readRecord();
        assertEquals("original", new String(out.value(), StandardCharsets.UTF_8));
        assertEquals("I can't help with that.", header(out.headers(), LlmProcessor.H_REFUSAL_REASON));
        assertTrue(output.isEmpty());
    }

    @Test
    void nonRetryableErrorGoesToDlq() {
        start(text -> {
            throw new LlmException("bad model", 400, false, null);
        });
        input.pipeInput(bytes("k"), bytes("original"));

        final TestRecord<byte[], byte[]> out = dlq.readRecord();
        assertEquals("original", new String(out.value(), StandardCharsets.UTF_8));
        assertEquals("openai", header(out.headers(), LlmProcessor.H_PROVIDER));
        assertEquals("bad model", header(out.headers(), LlmProcessor.H_ERROR_MESSAGE));
        assertEquals("400", header(out.headers(), LlmProcessor.H_ERROR_STATUS));
        assertEquals("false", header(out.headers(), LlmProcessor.H_ERROR_RETRYABLE));
    }

    @Test
    void retryableErrorGoesToDlqByDefault() {
        start(text -> {
            throw new LlmException("overloaded", 503, true, null);
        });
        input.pipeInput(bytes("k"), bytes("original"));
        assertEquals("true", header(dlq.readRecord().headers(), LlmProcessor.H_ERROR_RETRYABLE));
    }

    @Test
    void retryableErrorStopsWhenConfigured() {
        start(text -> {
            throw new LlmException("overloaded", 503, true, null);
        }, Map.of("RETRYABLE_ERROR_ACTION", "stop"));

        final StreamsException e = assertThrows(StreamsException.class, () -> input.pipeInput(bytes("k"), bytes("original")));
        assertInstanceOf(LlmException.class, e.getCause());
        assertTrue(dlq.isEmpty());
        assertTrue(output.isEmpty());
    }

    @Test
    void truncatedStructuredOutputGoesToDlq() {
        start(text -> new LlmResult("{\"categ", "gpt-5.5", "length", 5, 1024, "req_3", false, null, true),
                Map.of("LLM_OUTPUT_SCHEMA", "{\"type\":\"object\"}"));
        input.pipeInput(bytes("k"), bytes("original"));

        final TestRecord<byte[], byte[]> out = dlq.readRecord();
        assertEquals("original", new String(out.value(), StandardCharsets.UTF_8));
        assertEquals("length", header(out.headers(), LlmProcessor.H_STOP_REASON));
        assertTrue(output.isEmpty());
    }

    @Test
    void oversizedValueGoesToDlqWithoutCallingLlm() {
        start(text -> answer("x"), Map.of("MAX_INPUT_BYTES", "10"));
        input.pipeInput(bytes("k"), bytes("this value is longer than ten bytes"));

        assertTrue(header(dlq.readRecord().headers(), LlmProcessor.H_ERROR_MESSAGE).contains("exceeds MAX_INPUT_BYTES"));
        assertTrue(sent.isEmpty());
    }

    @Test
    void binaryValueGoesToDlqWithoutCallingLlm() {
        start(text -> answer("x"));
        input.pipeInput(bytes("k"), new byte[]{(byte) 0xC3, (byte) 0x28});

        assertEquals("Value is not valid UTF-8 text", header(dlq.readRecord().headers(), LlmProcessor.H_ERROR_MESSAGE));
        assertTrue(sent.isEmpty());
    }

    @Test
    void nullValueGoesToDlq() {
        start(text -> answer("x"));
        input.pipeInput(bytes("k"), (byte[]) null);

        final TestRecord<byte[], byte[]> out = dlq.readRecord();
        assertNull(out.value());
        assertEquals("Record has no value", header(out.headers(), LlmProcessor.H_ERROR_MESSAGE));
        assertTrue(sent.isEmpty());
    }
}
