package com.bigdata.demoFour;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 这个例子是判断订单是否正常，譬如说，订单的有效时间是30分钟，所以要在30分钟内完成支付，才算一次正常的支付。
 * 如果超过了30分钟，用户依然发起了支付动作，这个时候就是有问题的，要发出一条指令告诉用户该订单已经超时。
 * <p>
 * 1、超时不是过了设定时间，是下面来的一条数据超过设定时间
 * 2、如果第一个pattern匹配，超时之后第二个pattern一直不匹配，来一条报一次超时，当第二个pattern匹配上，在之后正常处理
 */
public class TestCEP {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /**
         *  接收source并将数据转换成一个tuple
         */
        DataStream<Tuple3<String, String, String>> myDataStream = env.addSource(new MySource()).map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {

                JSONObject json = JSON.parseObject(value);
                return new Tuple3<>(json.getString("userid"), json.getString("orderid"), json.getString("behave"));
            }
        });

        /**
         * 定义一个规则
         * 接受到behave是order以后，下一个动作必须是pay才算符合这个需求
         */
        Pattern<Tuple3<String, String, String>, Tuple3<String, String, String>> myPattern = Pattern.<Tuple3<String, String, String>>begin("start").where(new IterativeCondition<Tuple3<String, String, String>>() {
            @Override
            public boolean filter(Tuple3<String, String, String> value, Context<Tuple3<String, String, String>> ctx) throws Exception {
                System.out.println("value1:" + value);
                return value.f2.equals("order"); //behave
            }
        }).next("next").where(new IterativeCondition<Tuple3<String, String, String>>() {
            @Override
            public boolean filter(Tuple3<String, String, String> value, Context<Tuple3<String, String, String>> ctx) throws Exception {
                System.out.println("value2:" + value);
                return value.f2.equals("pay");  //behave
            }
        }).within(Time.seconds(3));//mock


        PatternStream<Tuple3<String, String, String>> pattern = CEP.pattern(myDataStream.keyBy(0), myPattern);

        //记录超时的订单
        OutputTag<String> outputTag = new OutputTag<String>("myOutput") {
        };

        SingleOutputStreamOperator<String> resultStream = pattern.select(outputTag,
                /**
                 * 超时的  对超时数据的方法
                 */
                new PatternTimeoutFunction<Tuple3<String, String, String>, String>() {
                    @Override
                    public String timeout(Map<String, List<Tuple3<String, String, String>>> pattern, long timeoutTimestamp) throws Exception {
                        System.out.println("pattern:" + pattern);

                        List<Tuple3<String, String, String>> startList = pattern.get("start");
                        Tuple3<String, String, String> tuple3 = startList.get(0);
                        return tuple3.toString() + "迟到的";
                    }
                }, new PatternSelectFunction<Tuple3<String, String, String>, String>() {
                    @Override
                    public String select(Map<String, List<Tuple3<String, String, String>>> pattern) throws Exception {
                        //匹配上第一个条件的
                        List<Tuple3<String, String, String>> startList = pattern.get("start");
                        //匹配上第二个条件的
                        List<Tuple3<String, String, String>> endList = pattern.get("next");

                        Tuple3<String, String, String> tuple3 = endList.get(0);  //只要规定时间完成订单的 pay
                        return tuple3.toString() + "正常";
                    }
                }
        );

        //输出匹配上规则的数据
        resultStream.print();

        //输出超时数据的流
        DataStream<String> sideOutput = resultStream.getSideOutput(outputTag);
        sideOutput.print();

        env.execute("Test CEP");
    }
}
//followedByAny 和 followedBy的区别：
//模式为begin("first").where(_.name='a').followedBy("second").where(.name='b')
//当数据为 a,c,b,b的时候，followedBy输出的模式是a,b   而followedByAny输出的模式是{a,b},{a,b}两组。

