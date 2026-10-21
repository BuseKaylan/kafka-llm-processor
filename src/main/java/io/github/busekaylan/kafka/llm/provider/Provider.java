package io.github.busekaylan.kafka.llm.provider;

import java.util.Locale;

/** Supported LLM providers. */
public enum Provider {
    ANTHROPIC("ANTHROPIC_API_KEY", "claude-opus-5-5"),
    OPENAI("OPENAI_API_KEY", null),
    GEMINI("GEMINI_API_KEY", null);

    private final String apiKeyVariable;
    private final String defaultModel;

    Provider(final String apiKeyVariable, final String defaultModel) {
        this.apiKeyVariable = apiKeyVariable;
        this.defaultModel = defaultModel;
    }

    /** The provider's conventional API key variable, accepted when LLM_API_KEY is not set. */
    public String apiKeyVariable() {
        return apiKeyVariable;
    }

    /** Default model id, or null when the model must be configured. */
    public String defaultModel() {
        return defaultModel;
    }

    public String id() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static Provider parse(final String value) {
        return valueOf(value.trim().toUpperCase(Locale.ROOT));
    }

    public LlmClient createClient(final LlmSettings settings) {
        return switch (this) {
            case ANTHROPIC -> new AnthropicLlmClient(settings);
            case OPENAI -> new OpenAiLlmClient(settings);
            case GEMINI -> new GeminiLlmClient(settings);
        };
    }
}
