package com.bigdata;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 *broadcast广播的流和原来的流connect，可以改变并行度之后每个流都可以获得，可以动态的传递参数
 * 可以叫做控制流
 */
public class BroadcastStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String[] NUMS = new String[]{"1", "2", "3", "4", "5", "6"};
        String[] ZI = new String[]{"A"};
        Integer[] a = new Integer[]{1};

        DataStreamSource<String> source1 = env.fromElements(NUMS);
        DataStreamSource<String> source2 = env.fromElements(ZI);
        DataStream<Integer> broadcast = env.fromElements(a).broadcast();

        //

        ConnectedStreams<String, Integer> connect = source2.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        }).connect(broadcast);

        //connect流处理是两个进一个出
        connect.flatMap(new CoFlatMapFunction<String, Integer, String>() {
            //第一个是source2
            @Override
            public void flatMap1(String value, Collector<String> out) throws Exception {

                out.collect(value);
            }

            //括号里面为第二个
            @Override
            public void flatMap2(Integer value, Collector<String> out) throws Exception {
                out.collect(String.valueOf(value));
            }
        }).setParallelism(2);

        env.execute("broadcast");
    }
}
