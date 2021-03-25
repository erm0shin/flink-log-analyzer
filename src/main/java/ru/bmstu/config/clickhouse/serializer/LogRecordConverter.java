package ru.bmstu.config.clickhouse.serializer;

import ru.bmstu.model.LogRecord;

public class LogRecordConverter {
    public static String toClickHouseInsertFormat(LogRecord logRecord) {
        return convertToCsv(logRecord);
    }

    private static String convertToCsv(LogRecord logRecord) {
        final StringBuilder builder = new StringBuilder();
        builder.append("(");

        // add a.str
        builder.append("'");
        builder.append(logRecord.getLevel());
        builder.append("', ");

        // add a.intger
        builder.append("'");
        builder.append(logRecord.getMessage());
        builder.append("')");
        return builder.toString();
    }
}
