package ru.bmstu;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import ru.bmstu.config.kafka.serializer.ProducerStringSerializationSchema;
import ru.bmstu.model.LogRecord;

import java.util.List;
import java.util.Map;
import java.util.Properties;

@SuppressWarnings("serial")
public class KafkaLogAnalyzer {

    private static final String INPUT_TOPIC = "input_logs";
    private static final String OUTPUT_TOPIC = "output_commands";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> logs = env.addSource(createKafkaConsumer());

        DataStream<LogRecord> logRecords = logs
                .flatMap(
                        new FlatMapFunction<String, LogRecord>() {
                            @Override
                            public void flatMap(String value, Collector<LogRecord> out) {
                                final int delimiterPosition = (!value.contains(":") ? 0 : value.indexOf(":"));
                                final String logLevel = value.substring(0, delimiterPosition);
                                final String logMessage = value.substring(delimiterPosition + 2);
                                out.collect(new LogRecord(logLevel, logMessage));
                            }
                        });

        PatternStream<LogRecord> filteredLogStream = CEP.pattern(logRecords, createLogFilter());

        DataStream<String> alerts = filteredLogStream.process(
                new PatternProcessFunction<LogRecord, String>() {
                    @Override
                    public void processMatch(
                            Map<String, List<LogRecord>> pattern,
                            Context ctx,
                            Collector<String> out
                    ) throws Exception {
                        out.collect("ALERT: " + pattern.values().toString());
                    }
                });

        alerts.addSink(createKafkaProducer());

        env.execute("Log File Analyzer");
    }

    private static Pattern<LogRecord, ?> createLogFilter() {
        return Pattern.<LogRecord>begin("start")
                .where(
                        new SimpleCondition<LogRecord>() {
                            @Override
                            public boolean filter(LogRecord event) {
                                return "ERROR".equals(event.getLevel());
                            }
                        }
                )
                .where(
                        new SimpleCondition<LogRecord>() {
                            @Override
                            public boolean filter(LogRecord event) {
                                return event.getMessage().toLowerCase().contains("attack");
                            }
                        }
                );
    }

    private static SourceFunction<String> createKafkaConsumer() {
        // get input data by connecting to the socket
        Properties inputProperties = new Properties();
        inputProperties.setProperty("bootstrap.servers", "localhost:9092");
        inputProperties.setProperty("group.id", "bmstu");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                INPUT_TOPIC,
                new SimpleStringSchema(),
                inputProperties
        );
        kafkaConsumer.setStartFromEarliest();     // start from the earliest record possible
        return kafkaConsumer;
    }

    private static SinkFunction<String> createKafkaProducer() {
        Properties outputProperties = new Properties();
        outputProperties.setProperty("bootstrap.servers", "localhost:9092");
        // write result commands to output topic
        return new FlinkKafkaProducer<>(
                OUTPUT_TOPIC,                                           // target topic
                new ProducerStringSerializationSchema(OUTPUT_TOPIC),    // serialization schema
                outputProperties,                                       // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE                // fault-tolerance
        );
    }

}