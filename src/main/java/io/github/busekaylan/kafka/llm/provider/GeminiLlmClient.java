package io.github.busekaylan.kafka.llm.provider;

import com.google.genai.Client;
import com.google.genai.errors.ApiException;
import com.google.genai.errors.GenAiIOException;
import com.google.genai.types.Candidate;
import com.google.genai.types.Content;
import com.google.genai.types.FinishReason;
import com.google.genai.types.GenerateContentConfig;
import com.google.genai.types.GenerateContentResponse;
import com.google.genai.types.GenerateContentResponseUsageMetadata;
import com.google.genai.types.HttpOptions;
import com.google.genai.types.HttpRetryOptions;
import com.google.genai.types.Part;
import com.google.genai.types.ThinkingConfig;
import com.google.genai.types.ThinkingLevel;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Google Gemini, through the official Google Gen AI Java SDK (Gemini Developer API). */
class GeminiLlmClient implements LlmClient {

    // Finish reasons that mean the answer was withheld by a safety or policy filter.
    private static final Set<FinishReason.Known> BLOCKED = Set.of(
            FinishReason.Known.SAFETY, FinishReason.Known.PROHIBITED_CONTENT, FinishReason.Known.BLOCKLIST,
            FinishReason.Known.SPII, FinishReason.Known.RECITATION, FinishReason.Known.IMAGE_SAFETY);

    private final LlmSettings settings;
    private final GenerateContentConfig config;
    private final Client client;

    GeminiLlmClient(final LlmSettings settings) {
        this.settings = settings;
        final HttpOptions.Builder http = HttpOptions.builder()
                .timeout((int) settings.timeout().toMillis())
                .retryOptions(HttpRetryOptions.builder()
                        .attempts(settings.maxRetries() + 1)
                        .httpStatusCodes(408, 429, 500, 502, 503, 504));
        if (settings.baseUrl() != null) {
            http.baseUrl(settings.baseUrl());
        }
        this.client = Client.builder().apiKey(settings.apiKey()).httpOptions(http.build()).build();
        this.config = buildConfig(settings);
    }

    @Override
    public LlmResult complete(final String content) {
        final GenerateContentResponse response;
        try {
            response = client.models.generateContent(settings.model(), LlmClient.document(content) + "\n\n" + settings.prompt(), config);
        } catch (final ApiException e) {
            throw new LlmException(e.getMessage(), e.code(), LlmException.isRetryableStatus(e.code()), e);
        } catch (final GenAiIOException e) {
            throw new LlmException(e.getMessage(), null, true, e);
        }

        final Optional<String> blockReason = response.promptFeedback()
                .flatMap(feedback -> feedback.blockReason())
                .map(reason -> reason.toString());
        final Optional<FinishReason> finishReason = response.candidates()
                .flatMap(candidates -> candidates.stream().findFirst())
                .flatMap(Candidate::finishReason);
        final boolean blocked = blockReason.isPresent()
                || finishReason.map(reason -> BLOCKED.contains(reason.knownEnum())).orElse(false);
        final String stopReason = finishReason.map(FinishReason::toString).orElse(blockReason.map(reason -> "BLOCKED").orElse(""));
        final Optional<GenerateContentResponseUsageMetadata> usage = response.usageMetadata();

        return new LlmResult(
                blocked ? "" : text(response),
                response.modelVersion().orElse(settings.model()),
                stopReason,
                usage.flatMap(GenerateContentResponseUsageMetadata::promptTokenCount).orElse(0),
                usage.flatMap(GenerateContentResponseUsageMetadata::candidatesTokenCount).orElse(0),
                response.responseId().orElse(null),
                blocked,
                blockReason.orElse(finishReason.filter(reason -> BLOCKED.contains(reason.knownEnum())).map(FinishReason::toString).orElse(null)),
                finishReason.map(reason -> reason.knownEnum() == FinishReason.Known.MAX_TOKENS).orElse(false));
    }

    /** Joins the text parts of the first candidate, skipping thought summaries. */
    private static String text(final GenerateContentResponse response) {
        return response.candidates()
                .flatMap(candidates -> candidates.stream().findFirst())
                .flatMap(Candidate::content)
                .flatMap(Content::parts)
                .map(parts -> parts.stream()
                        .filter(part -> !part.thought().orElse(false))
                        .map(part -> part.text().orElse(""))
                        .collect(Collectors.joining()))
                .orElse("");
    }

    static GenerateContentConfig buildConfig(final LlmSettings settings) {
        final GenerateContentConfig.Builder builder = GenerateContentConfig.builder()
                .maxOutputTokens((int) settings.maxTokens());
        if (settings.systemPrompt() != null) {
            builder.systemInstruction(Content.fromParts(Part.fromText(settings.systemPrompt())));
        }
        if (settings.hasEffort()) {
            builder.thinkingConfig(ThinkingConfig.builder().thinkingLevel(thinkingLevel(settings.effort())).build());
        }
        final Map<String, Object> schema = settings.schemaMap();
        if (schema != null) {
            builder.responseMimeType("application/json").responseJsonSchema(schema);
        }
        return builder.build();
    }

    /** Maps effort to Gemini's thinking levels, whose highest is HIGH. */
    static ThinkingLevel.Known thinkingLevel(final String effort) {
        return List.of("xhigh", "max").contains(effort)
                ? ThinkingLevel.Known.HIGH
                : ThinkingLevel.Known.valueOf(effort.toUpperCase(Locale.ROOT));
    }

    @Override
    public void close() {
        client.close();
    }
}
