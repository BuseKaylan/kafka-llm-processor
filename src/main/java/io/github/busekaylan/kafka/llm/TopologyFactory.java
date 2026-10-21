package io.github.busekaylan.kafka.llm;

import io.github.busekaylan.kafka.llm.provider.LlmClient;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.Topology;

/** input topic → LlmProcessor → output, refused and dead letter topics. */
public final class TopologyFactory {

    private TopologyFactory() {
    }

    public static Topology build(final ProcessorConfig config, final LlmClient llm) {
        final Topology topology = new Topology();
        topology.addSource("source", new ByteArrayDeserializer(), new ByteArrayDeserializer(), config.inputTopic());
        topology.addProcessor("llm", () -> new LlmProcessor(llm, config), "source");
        topology.addSink(LlmProcessor.OUTPUT, config.outputTopic(), new ByteArraySerializer(), new ByteArraySerializer(), "llm");
        topology.addSink(LlmProcessor.REFUSED, config.refusedTopic(), new ByteArraySerializer(), new ByteArraySerializer(), "llm");
        topology.addSink(LlmProcessor.DLQ, config.dlqTopic(), new ByteArraySerializer(), new ByteArraySerializer(), "llm");
        return topology;
    }
}
