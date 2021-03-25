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
import ru.bmstu.model.LogRecord;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
                                final int delimiterPosition = (!value.contains(":") ? 0 : value.indexOf(":"));
                                final String logLevel = value.substring(0, delimiterPosition);
                                final String logMessage = value.substring(delimiterPosition + 2);
                                out.collect(new LogRecord(service, logLevel, logMessage));
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

        alerts.addSink(cofigureKafkaProducer());

        filteredLogStream
                .process(
                        new PatternProcessFunction<LogRecord, LogRecord>() {
                            @Override
                            public void processMatch(
                                    Map<String, List<LogRecord>> pattern,
                                    Context ctx,
                                    Collector<LogRecord> out
                            ) throws Exception {
                                out.collect(pattern.values().stream().flatMap(Collection::stream).collect(Collectors.toList()).get(0));
                            }
                        })
                .map(LogRecordConverter::toClickHouseInsertFormat)
                .addSink(configureClickHouseProducer());

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

}