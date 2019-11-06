package com.bigdata;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 *  * 需求：
 * flink会从数据源中获取到用户的姓名
 * 最终需要把用户的姓名和年龄信息打印出来
 *
 * 所以就需要在中间map处理的时候，就需要获取用户的年龄信息
 * 建议将用户的关系数据集使用广播变量进行处理
 */
public class BroadcastVariable {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //1、准备广播变量的数据
        ArrayList<Tuple2<String, Integer>> broadData = new ArrayList<>();
        broadData.add(new Tuple2<>("python",18));
        broadData.add(new Tuple2<>("scala",20));
        broadData.add(new Tuple2<>("java",17));
        DataStreamSource<Tuple2<String, Integer>> dataBroad = env.fromCollection(broadData);

        //2、对需要广播的数据进行处理，将tuple2类型转换成hashMap类型
        SingleOutputStreamOperator<HashMap<String, Integer>> baseData = dataBroad.map(new MapFunction<Tuple2 <String, Integer>, HashMap <String, Integer>>() {
            @Override
            public HashMap <String, Integer> map(Tuple2 <String, Integer> value) throws Exception {
                HashMap <String, Integer> res = new HashMap <>();
                res.put(value.f0, value.f1);
                return res;
            }
        });

        //调广播变量，SingleOutputStreamOperator 为 DataStream
//        DataStream<HashMap<String, Integer>> broadcast = baseData.broadcast();

        String[] var = new String[]{"python", "java","java","kafka","scala","redis"};
        DataStreamSource<String> mainData = env.fromElements(var);

        SingleOutputStreamOperator<String> result = mainData.map(new RichMapFunction<String, String>() {
            List<HashMap <String, Integer>> broadCastMap = new ArrayList <HashMap <String, Integer>>();
            HashMap <String, Integer> allMap = new HashMap <String, Integer>();

            /**
             * 这个方法只会执行一次
             * 可以在这里实现一些初始化的功能
             *
             * 所以，就可以在open方法中获取广播变量数据
             *
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //3:获取广播数据
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("broadCastMapName");
                for (HashMap map : broadCastMap) {
                    allMap.putAll(map);
                }

            }

            @Override
            public String map(String value) throws Exception {
                Integer age = allMap.get(value);
                return value + "," + age;
            }
        })
                //只适用于DataSet
//                .withBroadcastSet(baseData,"broadCastMapName")
                ;

    }
}
