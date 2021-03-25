package ru.bmstu.model;

import java.util.Objects;

public class Command {

    private final String service;
    private final String command;

    public Command(String service, String command) {
        this.service = service;
        this.command = command;
    }

    public String getService() {
        return service;
    }

    public String getCommand() {
        return command;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Command command1 = (Command) o;
        return Objects.equals(service, command1.service) &&
                Objects.equals(command, command1.command);
    }

    @Override
    public int hashCode() {
        return Objects.hash(service, command);
    }

    @Override
    public String toString() {
        return "Command{" +
                "service='" + service + '\'' +
                ", command='" + command + '\'' +
                '}';
    }

}
