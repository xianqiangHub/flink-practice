package com.bigdata;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class DimensionTableJoin {

    public static void main(String[] args) {

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        FlinkKafkaConsumer<String> fc = new FlinkKafkaConsumer<>("jaa,hotitem", new SimpleStringSchema(), prop);
        fc.setStartFromLatest();
        DataStreamSource<String> source = bsEnv.addSource(fc);



    }
}
