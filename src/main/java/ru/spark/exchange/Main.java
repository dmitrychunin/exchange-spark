package ru.spark.exchange;

import lombok.SneakyThrows;
import ru.spark.exchange.consume.OrdersConsumer;

public class Main {
    @SneakyThrows
    public static void main(String[] args) {
        OrdersConsumer.start();
    }
}
