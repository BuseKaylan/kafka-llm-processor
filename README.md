# Kafka LLM Processor

[![CI](https://github.com/BuseKaylan/kafka-llm-processor/actions/workflows/ci.yml/badge.svg)](https://github.com/BuseKaylan/kafka-llm-processor/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
![Kafka](https://img.shields.io/badge/Kafka%20Streams-4.3-231f20)
![Java](https://img.shields.io/badge/Java-21-orange)

[Türkçe](README.tr.md)

A [Kafka Streams](https://kafka.apache.org/documentation/streams/) application that sends every record from a topic to a large language model and writes the answer to another topic. It works with **Anthropic Claude**, **OpenAI GPT** and **Google Gemini**.

Use it to classify, extract, summarize, translate or normalize text as it flows through Kafka, with no code to write: you pick a provider, write a prompt and, optionally, a JSON Schema.

![How kafka-llm-processor works: input topic, LLM call, output, refused and dead letter topics](docs/images/how-it-works.png)

> This is an independent community project. It is not affiliated with, endorsed by, or sponsored by Anthropic, OpenAI, Google or the Apache Software Foundation.

Looking for Apache NiFi? See [nifi-llm-processor](https://github.com/BuseKaylan/nifi-llm-processor).

## Features

- **One configuration, three providers.** Switch between Claude, GPT and Gemini by changing `LLM_PROVIDER` and `LLM_MODEL`. Each provider uses its official Java SDK.
- **Structured outputs on every provider.** Set a JSON Schema and the answer is valid JSON that matches it, ready for downstream consumers, ksqlDB or a sink connector.
- **Three output topics.** Answers go to the output topic. Declined requests go to a refused topic, and failures go to a dead letter topic with the original value, so nothing is lost silently.
- **Keys and headers are kept.** Record keys and existing headers pass through unchanged. The provider, model, token usage and request id are added as `llm.*` headers.
- **Retries and backoff.** Rate limits, server errors and network errors are retried with exponential backoff. After that, you choose: send the record to the DLQ and continue, or stop so it is processed again after a restart.
- **Safe defaults for slow calls.** Polling is tuned for requests that take seconds, so consumers don't get kicked out of the group mid-batch.
- **Configured with environment variables.** Any `KAFKA_*` variable is passed to Kafka Streams, so SASL, SSL and every other client setting work out of the box.
- **Stateless.** No state stores and no RocksDB. Scale by adding partitions and instances.

## Quick start with Docker Compose

The repository includes a demo that starts Kafka, creates the topics and classifies support tickets.

```bash
cp .env.example .env
```

In `.env`, set `LLM_PROVIDER` (`anthropic`, `openai` or `gemini`), `LLM_API_KEY`, and for OpenAI or Gemini also `LLM_MODEL`. Then start everything:

```bash
docker compose up --build
```

In a second terminal, send the sample tickets:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic tickets --reader-property parse.key=true --reader-property key.separator='|' < examples/tickets.txt
```

And read the results, with their headers:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic tickets.llm --from-beginning --formatter-property print.key=true --formatter-property print.headers=true
```

Each ticket comes back as JSON such as `{"category": "technical", "urgent": true}`.

![Classified tickets on the output topic, with their llm.* headers](docs/images/terminal-output.png)

*Output of the sample tickets on `tickets.llm`. This run used a local mock of the OpenAI API, so the request id is a placeholder.*

## Running it

**Docker image** (published on every release):

```bash
docker run --rm -e KAFKA_BOOTSTRAP_SERVERS=broker:9092 -e INPUT_TOPIC=tickets -e LLM_PROVIDER=openai -e LLM_MODEL=gpt-5.5 -e LLM_API_KEY=... -e LLM_PROMPT="Classify this support ticket." ghcr.io/busekaylan/kafka-llm-processor:latest
```

**Jar:** download `kafka-llm-processor-<version>.jar` from [Releases](https://github.com/BuseKaylan/kafka-llm-processor/releases), set the environment variables, and run `java -jar kafka-llm-processor-<version>.jar`.

The input topic must exist before the application starts. Create the output, refused and DLQ topics too, unless your brokers create topics automatically.

## Configuration

### Required

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka brokers, for example `broker:9092`.
- `INPUT_TOPIC`: the topic to read.
- `LLM_PROVIDER`: `anthropic`, `openai` or `gemini`.
- `LLM_API_KEY`: the provider's API key. The provider's own variable also works: `ANTHROPIC_API_KEY`, `OPENAI_API_KEY` or `GEMINI_API_KEY`.
- `LLM_MODEL`: required for `openai` and `gemini`, for example `gpt-5.5` or `gemini-flash-latest`. Defaults to `claude-opus-5-5` for `anthropic`.
- `LLM_PROMPT`: the instruction sent with every record.

### Topics

- `OUTPUT_TOPIC`: default `<input>.llm`.
- `REFUSED_TOPIC`: default `<input>.refused`.
- `DLQ_TOPIC`: default `<input>.dlq`.

### Model settings

- `LLM_SYSTEM_PROMPT`: optional role and rules for the model.
- `LLM_OUTPUT_SCHEMA`: a JSON Schema, inline. Or use `LLM_OUTPUT_SCHEMA_FILE` with a path to a file. OpenAI runs in strict mode, which needs `"additionalProperties": false` and every property listed in `required`.
- `LLM_EFFORT`: `not-set` (default, the model's own default), `low`, `medium`, `high`, `xhigh` or `max`. It maps to `effort` on Claude, `reasoning_effort` on OpenAI (`max` becomes `xhigh`) and `thinkingLevel` on Gemini (`xhigh` and `max` become `HIGH`). Leave it unset for models without reasoning controls.
- `LLM_MAX_TOKENS`: default `16000`.
- `LLM_BASE_URL`: optional API gateway, proxy or compatible endpoint.
- `LLM_TIMEOUT_SECONDS`: default `300`, per attempt.
- `LLM_MAX_RETRIES`: default `2`.
- `ANTHROPIC_REFUSAL_FALLBACK`: default `true`. On Claude models that support it (Opus 5.x, Fable 5.1, Sonnet 5.5), a declined request is rerouted server-side to a fallback model in the same call. Disable it when using a gateway that doesn't support it.

### Processing

- `MAX_INPUT_BYTES`: default `2097152` (2 MB). Larger values go to the DLQ without an API call. Content is never truncated.
- `RETRYABLE_ERROR_ACTION`: what to do when the API is still unavailable after retries. `dlq` (default) sends the record to the DLQ with `llm.error.retryable=true` and continues. `stop` stops the application without committing, so the record is processed again after a restart.
- `KAFKA_*`: any other Kafka Streams or client setting. The name is lowercased and `_` becomes `.`, so `KAFKA_SECURITY_PROTOCOL` becomes `security.protocol`.

Defaults that differ from Kafka's: `application.id=kafka-llm-processor`, `max.poll.records=10` and `max.poll.interval.ms=900000`, because each record can take a few seconds. Override them with `KAFKA_APPLICATION_ID`, `KAFKA_MAX_POLL_RECORDS` and `KAFKA_MAX_POLL_INTERVAL_MS`.

## Headers

Records on the output and refused topics get:

- `llm.provider`, `llm.model`
- `llm.stop.reason`: the provider's own value, for example `end_turn` (Claude), `stop` (OpenAI) or `STOP` (Gemini)
- `llm.usage.input.tokens`, `llm.usage.output.tokens`
- `llm.request.id`, useful for support requests
- `llm.refusal.reason`, on refused records, when the provider gives one
- `content-type`: `application/json` with a schema, otherwise `text/plain; charset=utf-8`

DLQ records keep their original value and get `llm.provider`, `llm.error.message`, `llm.error.retryable` and, for API errors, `llm.error.status`.

### What counts as refused

- **Claude:** stop reason `refusal`.
- **OpenAI:** the message has a `refusal`, or the finish reason is `content_filter`.
- **Gemini:** the prompt was blocked, or the finish reason is a safety or policy reason such as `SAFETY`, `PROHIBITED_CONTENT` or `RECITATION`.

## Delivery and scaling

- **At-least-once.** Offsets are committed after the result is written, so a crash can send a record to the model twice. Set `KAFKA_PROCESSING_GUARANTEE=exactly_once_v2` to make the Kafka side exactly-once. The API call itself can still repeat after a crash.
- **Ordering** is kept per partition.
- **Parallelism** is one request at a time per partition. To go faster, add partitions to the input topic, then raise `KAFKA_NUM_STREAM_THREADS` or run more instances with the same application id.
- **Cost.** Every record is one API call. For simple classification, use a small model and `LLM_EFFORT=low`.

## Building

Requires Java 21 and Maven 3.9+.

```bash
mvn clean verify
```

The jar is written to `target/`. The tests use Kafka's `TopologyTestDriver` and local mock HTTP servers that speak each provider's API, so they need neither a Kafka cluster nor API keys, and they cost nothing.

## License

[Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for trademark and third-party notices.
