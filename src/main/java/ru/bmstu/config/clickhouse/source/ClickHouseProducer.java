package ru.bmstu.config.clickhouse.source;

import ru.ivi.opensource.flinkclickhousesink.ClickHouseSink;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkConst;

import java.util.Properties;

public class ClickHouseProducer {
    public static ClickHouseSink configureClickHouseProducer(String tableName) {
        Properties props = new Properties();
        props.put(ClickHouseSinkConst.TARGET_TABLE_NAME, tableName);
        props.put(ClickHouseSinkConst.MAX_BUFFER_SIZE, "1");
        return new ClickHouseSink(props);
    }
}
