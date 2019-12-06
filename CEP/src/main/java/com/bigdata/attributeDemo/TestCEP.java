package com.bigdata.attributeDemo;

import com.bigdata.fileSource.ReadLineSource;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * https://juejin.im/post/5de1f32af265da05cc3190f9#heading-1
 */
public class TestCEP {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> stream = env.addSource(new ReadLineSource("threeData.txt"));
//最简单，单条的熟悉
//        Pattern pattern = Pattern.<String>begin("start").where(new IterativeCondition<String>() {
//            @Override
//            public boolean filter(String s, Context<String> context) {
//                return s.startsWith("x");
//            }
//        }).or(new IterativeCondition<String>() {
//            @Override
//            public boolean filter(String s, Context<String> context) throws Exception {
//                return s.startsWith("y");
//            }
//        });

        Pattern pattern = Pattern.<String>begin("start").where(new IterativeCondition<String>() {
            @Override
            public boolean filter(String s, Context<String> context) {
                return s.startsWith("a");
            }
        }).times(2, 3).next("middle").where(new IterativeCondition<String>() {
            @Override
            public boolean filter(String s, Context<String> context) throws Exception {
                return s.startsWith("b");
            }
        }).times(1, 2);


        CEP.pattern(stream, pattern).select(new PatternSelectFunction<String, String>() {

            //拿到每个pattern，保存的中间结果
            @Override
            public String select(Map<String, List<String>> pattern) throws Exception {

                return Arrays.toString(pattern.get("start").toArray());
            }

        }).addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) {
                System.out.println(value);
            }
        });

        env.execute(TestCEP.class.getSimpleName());
    }
/**
 * greedy()：如果当前处于Pattern1，但是出现了一条同时满足两个Pattern1和Pattern2条件的数据，在不加greedy()的情况下，会跳转到Pattern2，但是如果加了greedy()，则会留在Pattern1
 *
 */
}

