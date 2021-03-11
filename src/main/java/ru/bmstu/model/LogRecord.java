package ru.bmstu.model;

import java.util.Objects;

public class LogRecord {

    private final String level;
    private final String message;

    public LogRecord(String level, String message) {
        this.level = level;
        this.message = message;
    }

    public String getLevel() {
        return level;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogRecord logRecord = (LogRecord) o;
        return Objects.equals(level, logRecord.level) &&
                Objects.equals(message, logRecord.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(level, message);
    }

    @Override
    public String toString() {
        return "Event{" +
                "level='" + level + '\'' +
                ", message='" + message + '\'' +
                '}';
    }

}
