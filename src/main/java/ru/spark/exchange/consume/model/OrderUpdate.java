package ru.spark.exchange.consume.model;

import lombok.Data;

import java.util.Set;

public class OrderUpdate {
    private String e;
    private Long E;
    private String s;
    private Long U;
    private Long u;
//    todo добавить ограничение ровно на 2 элемента во вложенном массиве
    private Set<Set<String>> b;
    private Set<Set<String>> a;
}
