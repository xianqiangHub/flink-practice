package com.bigdata.etl;

/**
 * Flink目前对于外部Exactly-Once写支持提供了两种的sink，一个是Kafka-Sink，另一个是Hdfs-Sink，
 * 这两种sink实现的Exactly-Once都是基于Flink checkpoint提供的hook来实现的两阶段提交模式来保证的
 */
public class Test {

    public static void main(String[] args) {

    }
}
