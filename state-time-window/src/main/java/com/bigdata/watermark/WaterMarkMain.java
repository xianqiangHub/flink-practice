package com.bigdata.watermark;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 1、processtime的时间时处理数据机器的本地时间
 * 2、watermark表示数据处理的进度，用来触发窗口或者定时器执行，时间窗口不能一直等属于这个窗口的数据
 *  乱序一方面是网络传输导致一方面并行处理各机器的处理速度不同
 *
 *  watermark的流动：
 *  每到一个处理节点生成一个新的watermark向下流动
 *  下游节点来自上游一个数据流的取watermark最大值，（正常流动watermark是递增的）
 *  下游节点来自上游多个数据流，watermark取最小值
 *
 *  watermark的产生：
 *  1、自定义数据源用emitWatermark
 *  2、任意地方使用assignTimestampsAndWatermarks
 *      这样可以在生成watermark之前处理数据
 *
 *  wartmark触发：
 *  1、触发窗口的定时器
 *  2、触发定时器
 *
 *
 */
public class WaterMarkMain {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.1.104:9092");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("docker", new SimpleStringSchema(), props);
        consumer.setStartFromEarliest();
        DataStreamSource<String> stream = env.addSource(consumer);
        //********************************************************************************
        //BoundedOutOfOrdernessTimestampExtractor<T> implements AssignerWithPeriodicWatermarks
        //周期性的生成watermark是当前所有数据的最大时间戳 - 乱序承受度
        stream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.minutes(1)) {
            @Override
            public long extractTimestamp(String element) {
                return 0;
            }
        });

    }
}
