package com.bigdata;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class CEPDemo {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        ArrayList<LoginEvent> list = new ArrayList<>();
        list.add(new LoginEvent("1", "192.168.0.1", "fail", 1558430842L));
        list.add(new LoginEvent("1", "192.168.0.2", "fail", 1558430843L));
        list.add(new LoginEvent("1", "192.168.0.3", "fail", 1558430844L));
        list.add(new LoginEvent("2", "192.168.10.10", "success", 1558430845L));

        SingleOutputStreamOperator<LoginEvent> source = env.fromCollection(list).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LoginEvent>() {
            @Override
            public long extractAscendingTimestamp(LoginEvent element) {
                return element.timestamp;
            }
        });
        //*******************************************************************
//        正常：流量在预设的正常范围内；
//        警告：某数据中心在10秒内连续两次上报的流量超过认定的正常值；
//        报警：某数据中心在30秒内连续两次匹配警告；

        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("begin")
                .where(new IterativeCondition<LoginEvent>() {
                    //true for values that should be retained,
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return value.status.equals("fail");
                    }
                })
                .next("next")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return value.status.equals("fail");
                    }
                })
                .within(Time.seconds(10));

        PatternStream<LoginEvent> patternStream = CEP.pattern(source, pattern);

        SingleOutputStreamOperator<String> select = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {

                return null;
            }
        });


    }

}
