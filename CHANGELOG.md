# Changelog

## 1.0.0

First release.

- Kafka Streams application that sends each record to an LLM (Kafka 4.3, Java 21)
- Providers: Anthropic Claude, OpenAI GPT and Google Gemini, through their official Java SDKs
- Output, refused and dead letter topics; keys and headers are kept
- Structured outputs through a JSON Schema, for every provider
- Configurable handling of retryable errors: dead letter topic or stop
- Configuration through environment variables, with `KAFKA_*` passthrough
- Docker image and a Docker Compose demo
