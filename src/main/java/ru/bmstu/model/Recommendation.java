package ru.bmstu.model;

import java.util.Objects;

public class Recommendation {
    private final String service;
    private final String recommendation;

    public Recommendation(String service, String recommendation) {
        this.service = service;
        this.recommendation = recommendation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Recommendation that = (Recommendation) o;
        return Objects.equals(service, that.service) &&
                Objects.equals(recommendation, that.recommendation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(service, recommendation);
    }

    @Override
    public String toString() {
        return "Recommendation{" +
                "service='" + service + '\'' +
                ", recommendation='" + recommendation + '\'' +
                '}';
    }

}
