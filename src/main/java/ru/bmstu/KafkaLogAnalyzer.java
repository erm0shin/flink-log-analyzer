package ru.bmstu;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import ru.bmstu.config.clickhouse.serializer.LogRecordConverter;
import ru.bmstu.model.Command;
import ru.bmstu.model.LogRecord;
import ru.bmstu.model.Recommendation;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static ru.bmstu.config.clickhouse.properties.ClickHouseProperties.cofigureClickHouseProperties;
import static ru.bmstu.config.clickhouse.source.ClickHouseProducer.configureClickHouseProducer;
import static ru.bmstu.config.kafka.source.KafkaConsumer.cofigureKafkaConsumer;
import static ru.bmstu.config.kafka.source.KafkaProducer.cofigureKafkaProducer;

@SuppressWarnings("serial")
public class KafkaLogAnalyzer {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(cofigureClickHouseProperties());

        DataStream<String> logs = env.addSource(cofigureKafkaConsumer());

        DataStream<LogRecord> logRecords = logs
                .flatMap(
                        new FlatMapFunction<String, LogRecord>() {
                            @Override
                            public void flatMap(String value, Collector<LogRecord> out) {
                                final String[] logObjects = value.split(";");
                                out.collect(new LogRecord(logObjects[0], logObjects[1], logObjects[2]));
                            }
                        });

        PatternStream<LogRecord> detectedAttacks = CEP.pattern(logRecords, detectAttacks());
        DataStream<Command> alerts = detectedAttacks.process(
                new PatternProcessFunction<LogRecord, Command>() {
                    @Override
                    public void processMatch(
                            Map<String, List<LogRecord>> pattern,
                            Context ctx,
                            Collector<Command> out
                    ) throws Exception {
                        out.collect(
                                pattern.values().stream()
                                        .flatMap(Collection::stream)
                                        .map(logRecord -> new Command(
                                                logRecord.getService(),
                                                "ALERT: " + logRecord.getMessage()
                                        ))
                                        .findAny()
                                        .get()
                        );
                    }
                });
        alerts
                .addSink(cofigureKafkaProducer());
        alerts
                .map(LogRecordConverter::toClickHouseCommandFormat)
                .addSink(configureClickHouseProducer("test_commands"));

        PatternStream<LogRecord> detectedLeaks = CEP.pattern(logRecords, detectLeaks());
        detectedLeaks.process(
                new PatternProcessFunction<LogRecord, Recommendation>() {
                    @Override
                    public void processMatch(
                            Map<String, List<LogRecord>> pattern,
                            Context ctx,
                            Collector<Recommendation> out
                    ) throws Exception {
                        out.collect(
                                pattern.values().stream()
                                        .flatMap(Collection::stream)
                                        .map(logRecord -> new Recommendation(
                                                logRecord.getService(),
                                                "To fix: " + logRecord.getMessage()
                                        ))
                                        .findAny()
                                        .get()
                        );
                    }
                })
                .map(LogRecordConverter::toClickHouseRecommendationFormat)
                .addSink(configureClickHouseProducer("test_recommendations"));

        env.execute("Log File Analyzer");
    }

    private static Pattern<LogRecord, ?> detectAttacks() {
        return Pattern.<LogRecord>begin("commands")
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

    private static Pattern<LogRecord, ?> detectLeaks() {
        return Pattern.<LogRecord>begin("recommendation")
                .where(
                        new SimpleCondition<LogRecord>() {
                            @Override
                            public boolean filter(LogRecord event) {
                                return "WARNING".equals(event.getLevel());
                            }
                        }
                )
                .where(
                        new SimpleCondition<LogRecord>() {
                            @Override
                            public boolean filter(LogRecord event) {
                                return event.getMessage().toLowerCase().contains("leak");
                            }
                        }
                );
    }

}