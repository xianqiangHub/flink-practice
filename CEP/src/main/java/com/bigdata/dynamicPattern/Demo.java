package com.bigdata.dynamicPattern;

import com.bigdata.fileSource.ReadLineSource;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Flink CEP 是运行在 Flink Job 里的，而规则库是放在外部存储中的。首先，
 * 需要在运行的 Job 中能及时发现外部存储中规则的变化，即需要在 Job 中提供访问外部库的能力。其次，
 * 需要将规则库中变更的规则动态加载到 CEP 中，即把外部规则的描述解析成 Flink CEP 所能识别的 pattern 结构体。最后，
 * 把生成的 pattern 转化成 NFA，替换历史 NFA，这样对新到来的消息，就会使用新的规则进行匹配。
 * ..............
 * https://mp.weixin.qq.com/s/4dQYr-RXKBRdrhu6Y5dZdw
 */
public class Demo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> stream = env.addSource(new ReadLineSource("threeData.txt"));

        Pattern myPattern = Pattern.<String>begin("start").where(new IterativeCondition<String>() {
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
//-------------------------------------------------------------------------------------------------


        PatternStream patternStream = CEP.pattern(stream, myPattern);


        env.execute(Demo.class.getSimpleName());
    }
}
