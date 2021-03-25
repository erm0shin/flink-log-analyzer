package ru.bmstu.config.kafka.serializer;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import ru.bmstu.model.Command;

import java.nio.charset.StandardCharsets;

public class KafkaCommandSerializer implements KafkaSerializationSchema<Command> {

    private String topic;

    public KafkaCommandSerializer(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Command element, Long timestamp) {
        return new ProducerRecord<>(topic, element.toString().getBytes(StandardCharsets.UTF_8));
    }

}
