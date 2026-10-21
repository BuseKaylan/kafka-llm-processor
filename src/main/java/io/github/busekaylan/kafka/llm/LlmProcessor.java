package io.github.busekaylan.kafka.llm;

import io.github.busekaylan.kafka.llm.ProcessorConfig.RetryableErrorAction;
import io.github.busekaylan.kafka.llm.provider.LlmClient;
import io.github.busekaylan.kafka.llm.provider.LlmClient.LlmException;
import io.github.busekaylan.kafka.llm.provider.LlmClient.LlmResult;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Sends each record value to the LLM and forwards the result to one of three child sinks:
 * {@link #OUTPUT} with the model's answer, {@link #REFUSED} with the original value, or {@link #DLQ} with the
 * original value and error headers. Keys and existing headers are kept.
 */
public class LlmProcessor implements Processor<byte[], byte[], byte[], byte[]> {

    static final String OUTPUT = "output";
    static final String REFUSED = "refused";
    static final String DLQ = "dlq";

    static final String HEADER_PREFIX = "llm.";
    static final String H_PROVIDER = "llm.provider";
    static final String H_MODEL = "llm.model";
    static final String H_STOP_REASON = "llm.stop.reason";
    static final String H_INPUT_TOKENS = "llm.usage.input.tokens";
    static final String H_OUTPUT_TOKENS = "llm.usage.output.tokens";
    static final String H_REQUEST_ID = "llm.request.id";
    static final String H_REFUSAL_REASON = "llm.refusal.reason";
    static final String H_ERROR_MESSAGE = "llm.error.message";
    static final String H_ERROR_STATUS = "llm.error.status";
    static final String H_ERROR_RETRYABLE = "llm.error.retryable";
    static final String H_CONTENT_TYPE = "content-type";

    private static final Logger LOG = LoggerFactory.getLogger(LlmProcessor.class);

    private final LlmClient llm;
    private final ProcessorConfig config;
    private ProcessorContext<byte[], byte[]> context;

    public LlmProcessor(final LlmClient llm, final ProcessorConfig config) {
        this.llm = llm;
        this.config = config;
    }

    @Override
    public void init(final ProcessorContext<byte[], byte[]> context) {
        this.context = context;
    }

    @Override
    public void process(final Record<byte[], byte[]> record) {
        final byte[] value = record.value();
        if (value == null) {
            toDlq(record, "Record has no value", null, false);
            return;
        }
        if (value.length > config.maxInputBytes()) {
            toDlq(record, "Value size %d bytes exceeds MAX_INPUT_BYTES of %d".formatted(value.length, config.maxInputBytes()), null, false);
            return;
        }
        final String text;
        try {
            text = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(value))
                    .toString();
        } catch (final CharacterCodingException e) {
            toDlq(record, "Value is not valid UTF-8 text", null, false);
            return;
        }

        final LlmResult result;
        try {
            result = llm.complete(text);
        } catch (final LlmException e) {
            if (e.retryable() && config.retryableErrorAction() == RetryableErrorAction.STOP) {
                // Rethrow so Kafka Streams stops before committing; the record is processed again on restart.
                LOG.error("LLM API is unavailable (status={}); stopping because RETRYABLE_ERROR_ACTION=stop", e.status(), e);
                throw e;
            }
            LOG.warn("LLM request failed (status={}, retryable={}): {}", e.status(), e.retryable(), e.getMessage());
            toDlq(record, e.getMessage(), e.status(), e.retryable());
            return;
        }

        final Map<String, String> meta = baseMeta();
        meta.put(H_MODEL, result.model());
        meta.put(H_STOP_REASON, result.stopReason());
        meta.put(H_INPUT_TOKENS, String.valueOf(result.inputTokens()));
        meta.put(H_OUTPUT_TOKENS, String.valueOf(result.outputTokens()));
        meta.put(H_REQUEST_ID, result.requestId());

        if (result.refused()) {
            meta.put(H_REFUSAL_REASON, result.refusalReason());
            context.forward(record.withHeaders(headers(record.headers(), meta)), REFUSED);
            return;
        }

        if (config.llm().outputSchema() != null && result.truncated()) {
            meta.put(H_ERROR_MESSAGE, "Structured response was truncated by LLM_MAX_TOKENS; increase it");
            meta.put(H_ERROR_RETRYABLE, "false");
            context.forward(record.withHeaders(headers(record.headers(), meta)), DLQ);
            return;
        }

        meta.put(H_CONTENT_TYPE, config.llm().outputSchema() != null ? "application/json" : "text/plain; charset=utf-8");
        context.forward(record
                .withValue(result.text().getBytes(StandardCharsets.UTF_8))
                .withHeaders(headers(record.headers(), meta)), OUTPUT);
    }

    private Map<String, String> baseMeta() {
        final Map<String, String> meta = new LinkedHashMap<>();
        meta.put(H_PROVIDER, config.llm().provider().id());
        return meta;
    }

    private void toDlq(final Record<byte[], byte[]> record, final String message, final Integer status, final boolean retryable) {
        final Map<String, String> meta = baseMeta();
        meta.put(H_ERROR_MESSAGE, message);
        meta.put(H_ERROR_RETRYABLE, String.valueOf(retryable));
        if (status != null) {
            meta.put(H_ERROR_STATUS, String.valueOf(status));
        }
        context.forward(record.withHeaders(headers(record.headers(), meta)), DLQ);
    }

    /** Copies the input headers, drops stale llm.* headers from earlier runs, and adds the new ones. */
    private static Headers headers(final Headers original, final Map<String, String> meta) {
        final RecordHeaders headers = new RecordHeaders();
        for (final Header header : original) {
            if (!header.key().startsWith(HEADER_PREFIX) && !meta.containsKey(header.key())) {
                headers.add(header);
            }
        }
        meta.forEach((key, value) -> {
            if (value != null) {
                headers.add(key, value.getBytes(StandardCharsets.UTF_8));
            }
        });
        return headers;
    }
}
