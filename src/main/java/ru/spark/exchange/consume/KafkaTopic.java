package ru.spark.exchange.consume;


public enum KafkaTopic {
    ORDER("order_string3");
    private final String topicName;

    KafkaTopic(String topicName) {
        this.topicName = topicName;
    }

    public String getTopicName() {
        return topicName;
    }
}
