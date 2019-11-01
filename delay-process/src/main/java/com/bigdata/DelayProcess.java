package com.bigdata;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 在电商商品购买过程中有这样一些场景：用户点击下单，此时订单处于待支付状态，如果在2小时之后还处于待支付状态那么就将这笔订单取消，置为取消状态；用户收货之后可以对商品进行评价，如果在24小时内仍然没有评价，那么自动将用户对商品的评分设置为5星….等等，这样的场景都可以称之为延时处理场景，当数据发送出去了，不立刻进行处理，而是等待一段时间之后在处理，目前对于延时处理的方案也有很多，例如：
 * <p>
 * java中DelayQueue
 * 内部使用优先级队列方式存储消息体，存放的消息体实现Dealy接口，然后使用一个线程不断消费队列数据。
 * <p>
 * redis中SortedSet
 * 借助Redis的SortedSet数据结构，使用时间作为排序的方式，外部使用一个线程不断轮询该SortedSet。
 * <p>
 * 定时扫描数据库
 * 将延时触发的任务信息存储在数据库中，然后使用线程去轮序查询符合要求触发的定时任务。
 * ……
 * <p>
 * 在流处理中也经常会有一些定时触发的场景，例如定时监控报警等，并且时间窗口的触发也是通过延时调用触发，
 * 接下来了解flink中是如何实现延时处理。
 */
public class DelayProcess {

    /**
     * 服务器下线监控报警，服务器上下线都会发送一条消息，如果发送的是下线消息，
     * 在之后的5min内没有收到上线消息则循环发出警告，直到上线取消告警。
     * <p>
     * case class ServerMsg(serverId: String, isOnline: Boolean, timestamp: Long)
     */
    public static void main(String[] args) throws Exception {

        //加入所有服务器的所有数据都打到Kafka，创建数据源
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //数据是有时效性的，用处理时间，处理数据的机器 的时间到了 触发
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("topic", new SimpleStringSchema(), props);
        DataStreamSource<String> stream = env.addSource(consumer);

        SingleOutputStreamOperator<ServerMsg> startStream = stream.map(new MapFunction<String, ServerMsg>() {
            @Override
            public ServerMsg map(String value) throws Exception {
                String[] split = value.split(",");
                return new ServerMsg(split[0], Boolean.valueOf(split[1]), Long.valueOf(split[2]));
            }
        });

        //*************************************************************************************
        KeyedStream<ServerMsg, Tuple> keyStream = startStream.keyBy("serverId");
        //开始处理
        keyStream.process(new KeyedProcessFunction<Tuple, ServerMsg, String>() {

            private ValueState<String> serverState;
            private ValueState<Long> timeState;

            //open方法，初始化状态信息
            @Override
            public void open(Configuration parameters) throws Exception {

                serverState = getRuntimeContext().getState(new ValueStateDescriptor<String>("server-state", TypeInformation.of(String.class)));
                timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time-state", TypeInformation.of(Long.class)));
            }


            // processElement方法，处理每条流入的数据，如果收到的是offline状态，则注册一个ProcessingTime的定时器，
// 并且将服务器信息与定时时间存储状态中；
// 如果收到的是online状态并且状态中定时时间不为-1，则删除定时器并将状态时间置为-1
            @Override
            public void processElement(ServerMsg value, Context ctx, Collector<String> out) throws Exception {
                //上下线都会发送一条消息
                if (!value.isOnline) {
                    //数据为离线，设置timer
                    long monitorTime = ctx.timerService().currentProcessingTime() + 300000;
                    timeState.update(monitorTime);
                    //告警用
                    serverState.update(value.serverId);
                    ctx.timerService().registerProcessingTimeTimer(monitorTime);
                }

                //状态更新为-1 说明已经删过了timer
                if (value.isOnline && -1 != timeState.value()) {
                    //删除timer需要 保存时间状态
                    ctx.timerService().deleteProcessingTimeTimer(timeState.value());
                    timeState.update(-1L);
                }
            }


            // onTimer方法，定时回调的方法，触发报警并且注册下一个定时告警
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

                if (timestamp == timeState.value()) {
                    Long newMonitorTime = timestamp + 300000;
                    timeState.update(newMonitorTime);
                    ctx.timerService().registerProcessingTimeTimer(newMonitorTime);

//                    "告警:" + serverState.value() + " is offline, please restart"
                }

            }
        }).print();


        env.execute("delay");
    }

    public static class ServerMsg {
        public String serverId;
        public Boolean isOnline;
        public Long timestamp;

        public ServerMsg(String serverId, Boolean isOnline, Long timestamp) {
            this.serverId = serverId;
            this.isOnline = isOnline;
            this.timestamp = timestamp;
        }
    }
}
