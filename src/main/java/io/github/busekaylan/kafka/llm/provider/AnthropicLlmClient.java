package io.github.busekaylan.kafka.llm.provider;

import com.anthropic.client.AnthropicClient;
import com.anthropic.client.okhttp.AnthropicOkHttpClient;
import com.anthropic.core.JsonValue;
import com.anthropic.core.http.HttpResponseFor;
import com.anthropic.errors.AnthropicIoException;
import com.anthropic.errors.AnthropicServiceException;
import com.anthropic.models.messages.ContentBlockParam;
import com.anthropic.models.messages.Message;
import com.anthropic.models.messages.MessageCreateParams;
import com.anthropic.models.messages.OutputConfig;
import com.anthropic.models.messages.TextBlockParam;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Anthropic Claude, through the official Anthropic Java SDK. */
class AnthropicLlmClient implements LlmClient {

    static final String FALLBACK_BETA = "server-side-fallback-2026-07-01";
    // Server-side refusal fallback is only accepted for these model families on the Claude API.
    private static final List<String> FALLBACK_MODEL_PREFIXES = List.of(
            "claude-opus-5", "claude-fable-5-1", "claude-sonnet-5-5");

    private final LlmSettings settings;
    private final Map<String, Object> schema;
    private final AnthropicClient client;

    AnthropicLlmClient(final LlmSettings settings) {
        this.settings = settings;
        this.schema = settings.schemaMap();
        final AnthropicOkHttpClient.Builder builder = AnthropicOkHttpClient.builder()
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
        try (HttpResponseFor<Message> response = client.messages().withRawResponse().create(buildParams(content))) {
            final String requestId = response.headers().values("request-id").stream().findFirst().orElse(null);
            final Message message = response.parse();
            final String stopReason = message.stopReason().map(reason -> reason.asString()).orElse("");
            final String text = message.content().stream()
                    .flatMap(block -> block.text().stream())
                    .map(textBlock -> textBlock.text())
                    .collect(Collectors.joining());
            final String category = message.stopDetails()
                    .flatMap(details -> details.category())
                    .map(value -> value.asString())
                    .orElse(null);
            return new LlmResult(text, message.model().asString(), stopReason,
                    message.usage().inputTokens(), message.usage().outputTokens(), requestId,
                    "refusal".equals(stopReason), category, "max_tokens".equals(stopReason));
        } catch (final AnthropicServiceException e) {
            throw new LlmException(e.getMessage(), e.statusCode(), LlmException.isRetryableStatus(e.statusCode()), e);
        } catch (final AnthropicIoException e) {
            throw new LlmException(e.getMessage(), null, true, e);
        }
    }

    MessageCreateParams buildParams(final String content) {
        final MessageCreateParams.Builder builder = MessageCreateParams.builder()
                .model(settings.model())
                .maxTokens(settings.maxTokens());
        if (settings.systemPrompt() != null) {
            builder.system(settings.systemPrompt());
        }
        builder.addUserMessageOfBlockParams(List.of(
                ContentBlockParam.ofText(TextBlockParam.builder().text(LlmClient.document(content)).build()),
                ContentBlockParam.ofText(TextBlockParam.builder().text(settings.prompt()).build())));

        if (settings.hasEffort() || schema != null) {
            final OutputConfig.Builder outputConfig = OutputConfig.builder();
            if (settings.hasEffort()) {
                outputConfig.effort(OutputConfig.Effort.of(settings.effort()));
            }
            if (schema != null) {
                outputConfig.putAdditionalProperty("format", JsonValue.from(Map.of("type", "json_schema", "schema", schema)));
            }
            builder.outputConfig(outputConfig.build());
        }

        if (settings.anthropicRefusalFallback() && FALLBACK_MODEL_PREFIXES.stream().anyMatch(settings.model()::startsWith)) {
            builder.putAdditionalHeader("anthropic-beta", FALLBACK_BETA);
            builder.putAdditionalBodyProperty("fallbacks", JsonValue.from("default"));
        }
        return builder.build();
    }

    @Override
    public void close() {
        client.close();
    }
}
