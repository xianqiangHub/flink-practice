package com.bigdata.JoinOrCogroup;

import com.bigdata.JoinOrCogroup.utils.WindowJoinSampleData;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 流的关联有
 * JoinedStream & CoGroupedStreams
 * JoinedStreams在底层又调用了CoGroupedStream来实现Join功能。
 * 实际上着两者还是有区别的，首先co-group侧重的是group,是对同一个key上的两组集合进行操作，
 * 而join侧重的是pair,是对同一个key上的每对元素操作。
 * co-group比join更通用一些，因为join只是co-group的一个特例，所以join是可以基于co-group来实现的（当然有优化的空间）。
 * 而在co-group之外又提供了join接口是因为用户更熟悉join
 * <p>
 * connect和union
 * ①.ConnectedStreams只能连接两个流，而union可以多余两个流；
 * ②.ConnectedStreams连接的两个流类型可以不一致，而union连接的流的类型必须一致；
 * ③.ConnectedStreams会对两个流的数据应用不同的处理方法，并且双流之间可以共享状态。
 * 这再第一个流的输入会影响第二流时，会非常有用。
 */
public class WindowJoin {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {
        // parse the parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        final long windowSize = params.getLong("windowSize", 2000);
        final long rate = params.getLong("rate", 3L);

        System.out.println("Using windowSize=" + windowSize + ", data rate=" + rate);
        System.out.println("To customize example, use: WindowJoin [--windowSize <window-size-in-millis>] [--rate <elements-per-second>]");

        // obtain execution environment, run this example in "ingestion time"
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // create the data sources for both grades and salaries
        DataStream<Tuple2<String, Integer>> grades = WindowJoinSampleData.GradeSource.getSource(env, rate);
        DataStream<Tuple2<String, Integer>> salaries = WindowJoinSampleData.SalarySource.getSource(env, rate);

        // run the actual window join program
        // for testability, this functionality is in a separate method.
        DataStream<Tuple3<String, Integer, Integer>> joinedStream = runWindowJoin(grades, salaries, windowSize);

        // print the results with a single thread, rather than in parallel
        joinedStream.print().setParallelism(1);

        // execute program
        env.execute("Windowed Join Example");
    }

    public static DataStream<Tuple3<String, Integer, Integer>> runWindowJoin(
            DataStream<Tuple2<String, Integer>> grades,
            DataStream<Tuple2<String, Integer>> salaries,
            long windowSize) {
//        JoinedStreams<Tuple2<String, Integer>, Tuple2<String, Integer>> join = grades.join(salaries);
        return grades.join(salaries)
                //where 和 equal 是提取来个流关联字段  两个流的jion是两个表的join
                .where(new NameKeySelector())
                .equalTo(new NameKeySelector())

                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                //T1, T2, T从
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {

                    @Override
                    public Tuple3<String, Integer, Integer> join(
                            Tuple2<String, Integer> first,
                            Tuple2<String, Integer> second) {
                        return new Tuple3<String, Integer, Integer>(first.f0, first.f1, second.f1);
                    }
                });
    }

    private static class NameKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
        @Override
        public String getKey(Tuple2<String, Integer> value) {
            return value.f0;
        }
    }
}
