package com.bigdata.demoOne;

import org.apache.flink.api.java.functions.KeySelector;
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

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        ArrayList<LoginEvent> list = new ArrayList<>();
        list.add(new LoginEvent("1", "192.168.0.1", "fail", 1558430842L));
//        list.add(new LoginEvent("1", "192.168.0.1", "fail", 1558430843L));
        list.add(new LoginEvent("1", "192.168.0.1", "fail", 1558430844L));
        list.add(new LoginEvent("4", "192.168.10.10", "success", 1558430845L));

        SingleOutputStreamOperator<LoginEvent> source = env.fromCollection(list).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LoginEvent>() {
            @Override
            public long extractAscendingTimestamp(LoginEvent element) {
                return element.timestamp;
            }
        });


        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("begin")
                .where(new IterativeCondition<LoginEvent>() {
                    //true for values that should be retained,
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        System.out.println("begin" + value.id);
                        return value.status.equals("fail");
                    }
                })
                .next("next")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        System.out.println("next" + value.id);
                        return value.status.equals("fail");
                    }
                })
                .within(Time.seconds(10));

        PatternStream<LoginEvent> patternStream = CEP.pattern(source.keyBy(new KeySelector<LoginEvent, String>() {
            @Override
            public String getKey(LoginEvent value) throws Exception {
                return value.id;
            }
        }), pattern);

        SingleOutputStreamOperator<String> select = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {

                List<LoginEvent> first = pattern.get("begin");
                for (LoginEvent loginEvent : first) {
                    System.out.println("first" + loginEvent.ip);
                }
                List<LoginEvent> second = pattern.get("next");

                for (LoginEvent loginEvent : second) {
                    System.out.println("second" + loginEvent.ip);
                }

                return "";
            }
        });

        select.print();

        env.execute("cep");
    }

}
