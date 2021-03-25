package ru.bmstu.config.kafka.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import static ru.bmstu.config.kafka.properties.KafkaProperties.configureInputKafkaProperties;

public class KafkaConsumer {
    private static final String INPUT_TOPIC = "input_logs";

    public static FlinkKafkaConsumer<String> cofigureKafkaConsumer() {
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                INPUT_TOPIC,
                new SimpleStringSchema(),
                configureInputKafkaProperties()
        );
        kafkaConsumer.setStartFromEarliest();     // start from the earliest record possible
        return kafkaConsumer;
    }
}
