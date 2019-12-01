package com.bigdata.demoTwo;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CEPTimeoutEventJob {
    private static final String LOCAL_KAFKA_BROKER = "192.168.1.104:9092";
    private static final String GROUP_ID = CEPTimeoutEventJob.class.getSimpleName();
    private static final String GROUP_TOPIC = GROUP_ID;

    public static void main(String[] args) throws Exception {
        // 参数
        ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 使用事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000);

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 10000));

        // 不使用POJO的时间  根据进入处理数据机器的时间为时间戳
//        final AssignerWithPeriodicWatermarks extractor = new IngestionTimeExtractor<POJO>();

        // 与Kafka Topic的Partition保持一致
        env.setParallelism(3);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER);
        kafkaProps.setProperty("group.id", GROUP_ID);

        // 接入Kafka的消息
        FlinkKafkaConsumer<POJO> consumer = new FlinkKafkaConsumer<>(GROUP_TOPIC, new POJOSchema(), kafkaProps);
        DataStream<POJO> pojoDataStream = env.addSource(consumer)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<POJO>() {
                    private static final long serialVersionUID = -6850937707597650098L;

                    @Override
                    public long extractAscendingTimestamp(POJO element) {
                        return element.getLogTime();
                    }
                });

        pojoDataStream.print();

        // 根据主键aid分组 即对每一个POJO事件进行匹配检测【不同类型的POJO，可以采用不同的within时间】
        // 1.
        DataStream<POJO> keyedPojos = pojoDataStream
                .keyBy("aid");

        // 从初始化到终态-一个完整的POJO事件序列
        // 2.
        Pattern<POJO, POJO> completedPojo =
                Pattern.<POJO>begin("init")
                        .where(new SimpleCondition<POJO>() {
                            private static final long serialVersionUID = 3653480011761612882L;

                            @Override
                            public boolean filter(POJO pojo) throws Exception {
                                return "02".equals(pojo.getAstatus());
                            }
                        })
//                        .followedBy("end")
                        .next("end")
                        .where(new SimpleCondition<POJO>() {
                            private static final long serialVersionUID = 3232276316328373748L;

                            @Override
                            public boolean filter(POJO pojo) throws Exception {
                                return "00".equals(pojo.getAstatus()) || "01".equals(pojo.getAstatus());
                            }
                        })
                        .within(Time.minutes(1));

        // 找出1分钟内【便于测试】都没有到终态的事件aid
        // 如果针对不同类型有不同within时间，比如有的是超时1分钟，有的可能是超时1个小时 则生成多个PatternStream
        // 3.
        PatternStream<POJO> patternStream = CEP.pattern(keyedPojos, completedPojo);

        // 定义侧面输出timedout
        // 4.
        OutputTag<POJO> timedout = new OutputTag<POJO>("timedout") {
            private static final long serialVersionUID = 773503794597666247L;
        };

        // OutputTag<L> timeoutOutputTag, PatternFlatTimeoutFunction<T, L> patternFlatTimeoutFunction, PatternFlatSelectFunction<T, R> patternFlatSelectFunction
        // 5.
        SingleOutputStreamOperator timeoutPojos = patternStream.flatSelect(
                timedout,
                new POJOTimedOut(),
                new FlatSelectNothing()
        );

        // 打印输出超时的POJO
        // 6.7.
        timeoutPojos.getSideOutput(timedout).print();
        timeoutPojos.print();

        env.execute(CEPTimeoutEventJob.class.getSimpleName());
    }

    /**
     * 把超时的事件收集起来
     */
    public static class POJOTimedOut implements PatternFlatTimeoutFunction<POJO, POJO> {
        private static final long serialVersionUID = -4214641891396057732L;

        @Override
        public void timeout(Map<String, List<POJO>> map, long l, Collector<POJO> collector) throws Exception {
            if (null != map.get("init")) {
                for (POJO pojoInit : map.get("init")) {
                    System.out.println("timeout init:" + pojoInit.getAid());
                    collector.collect(pojoInit);
                }
            }
            // 因为end超时了，还没收到end，所以这里是拿不到end的
            System.out.println("timeout end: " + map.get("end"));
        }
    }

    /**
     * 通常什么都不做，但也可以把所有匹配到的事件发往下游；如果是宽松临近，被忽略或穿透的事件就没办法选中发往下游了
     * 一分钟时间内走完init和end的数据
     *
     * @param <T>
     */
    public static class FlatSelectNothing<T> implements PatternFlatSelectFunction<T, T> {
        private static final long serialVersionUID = -3029589950677623844L;

        @Override
        public void flatSelect(Map<String, List<T>> pattern, Collector<T> collector) {
            System.out.println("flatSelect: " + pattern);
        }
    }
}