package com.bigdata;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * Assigner、Trigger、Function、Evictor
 *
 * countwindow:
 * 窗口要有assign和trigger 窗口函数
 * 窗口的中间数据是存储在状态中,
 * 一个operator状态的唯一性通过StateDesc、Key、Namespace, 窗口中namespace 就是window
 * 首先获取ReduceState 计数器，表示当前key 的数量，每处理一条数据就进行+1操作， 当count 达到执行触发量就会将当前key 计数state 清空，下次从0开始计数，并且触发窗口操作。
 * 这种状态计数器也会在checkpoint时候被储存，使其具有容错性，能够在任务失败中恢复，达到精确计数。
 *
 *
 *
 */
public class SourceWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.1.104:9092");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("docker", new SimpleStringSchema(), props);
        consumer.setStartFromEarliest();
        DataStreamSource<String> stream = env.addSource(consumer);

////        stream.keyBy("key").countWindow(5)
//
//        stream.map(new MapFunction<String, Tuple2<String,String>>() {
//            @Override
//            public Tuple2 map(String value) throws Exception {
//                return new Tuple2(value,"a");
//            }
//        })
////                keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(1),Time.days(1)))
////                timeWindow(Time.seconds(5)).max(1);


        env.execute("a");
    }
}
