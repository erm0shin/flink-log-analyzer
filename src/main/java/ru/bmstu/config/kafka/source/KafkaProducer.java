package ru.bmstu.config.kafka.source;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import ru.bmstu.config.kafka.serializer.KafkaStringSerializer;

import static ru.bmstu.config.kafka.properties.KafkaProperties.configureOutputKafkaProperties;

public class KafkaProducer {
    private static final String OUTPUT_TOPIC = "output_commands";

    public static FlinkKafkaProducer<String> cofigureKafkaProducer() {
        return new FlinkKafkaProducer<>(
                OUTPUT_TOPIC,
                new KafkaStringSerializer(OUTPUT_TOPIC),
                configureOutputKafkaProperties(),
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }
}
