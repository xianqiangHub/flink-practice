package com.bigdata.io;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * Async I/O 的前提是需要一个支持异步请求的客户端。
 * 当然，没有异步请求客户端的话也可以将同步客户端丢到线程池中执行作为异步客户端。
 */
//异步IO实现读入redis中的维表数据
public class AsyncIO {

    public static void main(String[] args) throws Exception {
        System.out.println("===============》 flink任务开始  ==============》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置检查点时间间隔
        env.enableCheckpointing(5000);
        //设置检查点模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        System.out.println("===============》 开始读取kafka中的数据  ==============》");
        SingleOutputStreamOperator<String> kafkaData = env.readTextFile("/Users/apple/app/ab.txt");

        //AsyncDataStream.unorderedWait
        // A helper class to apply {@link AsyncFunction} to a data stream.
        //从KafkaData流的每一个元素，去redis异步的获取值，返回
        SingleOutputStreamOperator<String> unorderedWait = AsyncDataStream.unorderedWait(kafkaData, new AsyncReadRedis(), 1000, TimeUnit.MICROSECONDS, 100);
        unorderedWait.print();
        //设置程序名称
        env.execute("data_to_redis");

    }
}
