package com.bigdata;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;


/**
 *  Setting timers is only supported on a keyed streams.
 *  processing-time/event-time timer都由TimerService在内部维护并排队等待执行，仅在keyed stream中有效。
 *
 * 由于Flink对(每个key+timestamp)只维护一个计时器。如果为相同的timestamp注册了多个timer ，则只调用onTimer()方法一次。
 *
 * Flink保证同步调用onTimer()和processElement() 。因此用户不必担心状态的并发修改。
 *
 * 注册使用registerProcessingTimeTimer，传入的是一个触发的时间戳，内部会将获取到当前的Key、VoidNamespace 、
 * timestamp封装成为一个InternalTimer对象存入优先级队列中
 *
 * 会不断遍历优先级队列触发任务
 * flink会在checkpoint过程中将优先级队列中的数据一起持久化到hdfs上
 *
 *
 * InternalTimer对象中的key和namespace为notnull，所以process
 */
public class ProcessTime {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.1.104:9092");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("docker", new SimpleStringSchema(), props);
        consumer.setStartFromEarliest();
        DataStreamSource<String> stream = env.addSource(consumer);


        stream.process(new ProcessFunction<String, String>() {

            @Override
            public void open(Configuration parameters) throws Exception {

                RuntimeContext runtimeContext = getRuntimeContext();



            }

            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

                ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+3000);
                System.out.println(ctx.timerService().currentProcessingTime());
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

//                ctx.timerService().deleteProcessingTimeTimer(3000);
                System.out.println("执行了");
            }
        }).print();


        env.execute("time");
    }
}
