package com.bigdata;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

/**
 * ProcessingTime  定时定量输出的实例
 * <p>
 * 使用flink自带定时功能，首先我们得能够获取到ProcessingTimeService这个对象，
 * 但是该对象的获取只能在AbstractStreamOperator通过getProcessingTimeService方法获取到，
 * 那么我们可以自定义一个StreamOperator 继承AbstractStreamOperator：
 */
public class SinkTimer {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("topic", new SimpleStringSchema(), props);
        DataStreamSource<String> stream = env.addSource(consumer);


        stream.transform("mySink", TypeInformation.of(String.class), new BatchIntervalSink(12, 33L)).setParallelism(1);

        //java的定时调度器，自定义source是定时工作
//        Timer timer = new Timer();
//        timer.schedule(new TimerTask() {
//            @Override
//            public void run() {
//
//            }
//        },1000);

        env.execute("excutor");
    }
}
