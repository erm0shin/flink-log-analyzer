package ru.bmstu.config.clickhouse.properties;

import org.apache.flink.api.java.utils.ParameterTool;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkConst;

import java.util.HashMap;
import java.util.Map;

public class ClickHouseProperties {
    public static ParameterTool cofigureClickHouseProperties() {
        Map<String, String> globalParameters = new HashMap<>();

// ClickHouse cluster properties
        globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_HOSTS, "localhost:8123");
        globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_USER, "default");
        globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_PASSWORD, "");

//// sink common
        globalParameters.put(ClickHouseSinkConst.TIMEOUT_SEC, "5");
        globalParameters.put(ClickHouseSinkConst.FAILED_RECORDS_PATH, "/errors");
        globalParameters.put(ClickHouseSinkConst.NUM_WRITERS, "1");
        globalParameters.put(ClickHouseSinkConst.NUM_RETRIES, "1");
        globalParameters.put(ClickHouseSinkConst.QUEUE_MAX_CAPACITY, "1");
        globalParameters.put(ClickHouseSinkConst.IGNORING_CLICKHOUSE_SENDING_EXCEPTION_ENABLED, "false");

// set global paramaters
        return ParameterTool.fromMap(globalParameters);
    }
}
