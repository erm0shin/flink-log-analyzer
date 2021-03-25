package ru.bmstu.config.kafka.properties;

import java.util.Properties;

public class KafkaProperties {
    public static Properties configureInputKafkaProperties() {
        // get input data by connecting to the socket
        Properties inputProperties = new Properties();
        inputProperties.setProperty("bootstrap.servers", "localhost:9092");
        inputProperties.setProperty("group.id", "bmstu");
        return inputProperties;
    }

    public static Properties configureOutputKafkaProperties() {
        Properties outputProperties = new Properties();
        outputProperties.setProperty("bootstrap.servers", "localhost:9092");
        return outputProperties;
    }
}
