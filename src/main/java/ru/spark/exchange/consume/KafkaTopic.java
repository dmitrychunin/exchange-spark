package ru.spark.exchange.consume;


public enum KafkaTopic {
    ORDER("order");
    private final String topicName;

    KafkaTopic(String topicName) {
        this.topicName = topicName;
    }

    public String getTopicName() {
        return topicName;
    }
}
