package io.github.busekaylan.kafka.llm.provider;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.core.JsonValue;
import com.openai.core.http.HttpResponseFor;
import com.openai.errors.OpenAIIoException;
import com.openai.errors.OpenAIServiceException;
import com.openai.models.ReasoningEffort;
import com.openai.models.ResponseFormatJsonSchema;
import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.chat.completions.ChatCompletionMessage;

import java.util.Map;

/** OpenAI GPT models, through the official OpenAI Java SDK (Chat Completions API). */
class OpenAiLlmClient implements LlmClient {

    private final LlmSettings settings;
    private final Map<String, Object> schema;
    private final OpenAIClient client;

    OpenAiLlmClient(final LlmSettings settings) {
        this.settings = settings;
        this.schema = settings.schemaMap();
        final OpenAIOkHttpClient.Builder builder = OpenAIOkHttpClient.builder()
                .apiKey(settings.apiKey())
                .timeout(settings.timeout())
                .maxRetries(settings.maxRetries());
        if (settings.baseUrl() != null) {
            builder.baseUrl(settings.baseUrl());
        }
        this.client = builder.build();
    }

    @Override
    public LlmResult complete(final String content) {
        try (HttpResponseFor<ChatCompletion> response = client.chat().completions().withRawResponse().create(buildParams(content))) {
            final String requestId = response.requestId().orElse(null);
            final ChatCompletion completion = response.parse();
            final ChatCompletion.Choice choice = completion.choices().getFirst();
            final ChatCompletionMessage message = choice.message();
            final String finishReason = choice.finishReason().asString();
            final String refusal = message.refusal().orElse(null);
            final long inputTokens = completion.usage().map(usage -> usage.promptTokens()).orElse(0L);
            final long outputTokens = completion.usage().map(usage -> usage.completionTokens()).orElse(0L);
            return new LlmResult(message.content().orElse(""), completion.model(), finishReason,
                    inputTokens, outputTokens, requestId,
                    refusal != null || "content_filter".equals(finishReason), refusal, "length".equals(finishReason));
        } catch (final OpenAIServiceException e) {
            throw new LlmException(e.getMessage(), e.statusCode(), LlmException.isRetryableStatus(e.statusCode()), e);
        } catch (final OpenAIIoException e) {
            throw new LlmException(e.getMessage(), null, true, e);
        }
    }

    ChatCompletionCreateParams buildParams(final String content) {
        final ChatCompletionCreateParams.Builder builder = ChatCompletionCreateParams.builder()
                .model(settings.model())
                .maxCompletionTokens(settings.maxTokens());
        if (settings.systemPrompt() != null) {
            builder.addSystemMessage(settings.systemPrompt());
        }
        builder.addUserMessage(LlmClient.document(content) + "\n\n" + settings.prompt());

        if (settings.hasEffort()) {
            builder.reasoningEffort(ReasoningEffort.of(effort(settings.effort())));
        }
        if (schema != null) {
            final ResponseFormatJsonSchema.JsonSchema.Schema.Builder schemaBuilder = ResponseFormatJsonSchema.JsonSchema.Schema.builder();
            schema.forEach((key, value) -> schemaBuilder.putAdditionalProperty(key, JsonValue.from(value)));
            builder.responseFormat(ResponseFormatJsonSchema.builder()
                    .jsonSchema(ResponseFormatJsonSchema.JsonSchema.builder()
                            .name("output")
                            .strict(true)
                            .schema(schemaBuilder.build())
                            .build())
                    .build());
        }
        return builder.build();
    }

    /** OpenAI's highest reasoning effort is xhigh. */
    static String effort(final String effort) {
        return "max".equals(effort) ? "xhigh" : effort;
    }

    @Override
    public void close() {
        client.close();
    }
}
