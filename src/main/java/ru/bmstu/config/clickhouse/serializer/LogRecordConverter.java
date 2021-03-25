package ru.bmstu.config.clickhouse.serializer;

import ru.bmstu.model.Command;
import ru.bmstu.model.Recommendation;

public class LogRecordConverter {

    public static String toClickHouseCommandFormat(Command command) {
        final StringBuilder builder = new StringBuilder();
        builder.append("(");

        // add a.str
        builder.append("'");
        builder.append(command.getService());
        builder.append("', ");

        // add a.intger
        builder.append("'");
        builder.append(command.getCommand());
        builder.append("')");
        return builder.toString();
    }

    public static String toClickHouseRecommendationFormat(Recommendation recommendation) {
        final StringBuilder builder = new StringBuilder();
        builder.append("(");

        // add a.str
        builder.append("'");
        builder.append(recommendation.getService());
        builder.append("', ");

        // add a.intger
        builder.append("'");
        builder.append(recommendation.getRecommendation());
        builder.append("')");
        return builder.toString();
    }

}
