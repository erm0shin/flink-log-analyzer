package ru.bmstu.config.kafka.serializer;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

public class KafkaStringSerializer implements KafkaSerializationSchema<String> {

    private String topic;

    public KafkaStringSerializer(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String element, Long timestamp) {
        return new ProducerRecord<>(topic, element.getBytes(StandardCharsets.UTF_8));
    }

}
