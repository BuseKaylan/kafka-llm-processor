package io.github.busekaylan.kafka.llm.provider;

/** Sends one record's text to an LLM. Implementations must be thread-safe. */
public interface LlmClient extends AutoCloseable {

    /**
     * Sends the content to the model together with the configured prompt.
     *
     * @throws LlmException when the API call fails after the client's own retries
     */
    LlmResult complete(String content);

    @Override
    default void close() {
    }

    /** Wraps the record content in document tags; long content goes before the instruction. */
    static String document(final String content) {
        return "<document>\n" + content + "\n</document>";
    }

    /**
     * The model's answer, normalized across providers.
     *
     * @param stopReason    the provider's own finish reason, for example {@code end_turn}, {@code stop} or {@code STOP}
     * @param refused       the model or its safety system declined to answer
     * @param refusalReason provider-specific detail when refused, if any
     * @param truncated     the answer was cut off by the output token limit
     */
    record LlmResult(String text, String model, String stopReason, long inputTokens, long outputTokens,
                     String requestId, boolean refused, String refusalReason, boolean truncated) {
    }

    /** A failed API call. {@code status} is null for network errors. */
    class LlmException extends RuntimeException {
        private final Integer status;
        private final boolean retryable;

        public LlmException(final String message, final Integer status, final boolean retryable, final Throwable cause) {
            super(message, cause);
            this.status = status;
            this.retryable = retryable;
        }

        public Integer status() {
            return status;
        }

        public boolean retryable() {
            return retryable;
        }

        static boolean isRetryableStatus(final int status) {
            return status == 408 || status == 409 || status == 429 || status >= 500;
        }
    }
}
