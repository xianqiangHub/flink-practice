package com.bigdata.topN;

import com.bigdata.fileSource.ReadLineSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.collection.Iterable;

import java.util.TreeMap;

/**
 * windowAll之后所有数据都到一个窗口了，并行度为 1，产生热点问题
 * 解决：可以先分组，计算每一组的topN，在windowAll求所有的topN
 */
public class WindowAllTopN {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> stream = env.addSource(new ReadLineSource("windowAll.txt"));

        SingleOutputStreamOperator<Tuple2<String, Integer>> source = stream.flatMap(new LineSplitter()).keyBy(0).sum(1);

        source.keyBy(new TupleKeySelectorByStart())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .process(new MykeyTopFunction())
                .print();

        env.execute(WindowAllTopN.class.getSimpleName());
    }

    private static class MykeyTopFunction extends
            ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {

        }
    }

    private static class TupleKeySelectorByStart implements
            KeySelector<Tuple2<String, Integer>, String> {

        @Override
        public String getKey(Tuple2<String, Integer> value) throws Exception {
            // TODO Auto-generated method stub
            return value.f0.substring(0, 1);
        }

    }

    private static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split(",");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
