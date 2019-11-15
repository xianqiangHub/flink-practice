package com.bigdata.JoinAndCogroup;

import com.bigdata.JoinAndCogroup.utils.WindowJoinSampleData;
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
 * 在window之前调用的cogroup，和cogroup一样，join是cogroup的特例
 * 对keywindow触发的数据，也是在apply拆分到不同的list，
 * 之后通过flatJoinCoGroupFunction,笛卡尔积生成，传给自定义的function
 * <p>
 * //From JoinedCoGroupFunction::CoGroup
 *
 * @Override public void coGroup(Iterable<T1> first, Iterable<T2> second, Collector<T> out) throws Exception {
 * // 双层循环产生数据的笛卡尔积
 * for (T1 val1: first) {
 * for (T2 val2: second) {
 * out.collect(wrappedFunction.join(val1, val2));
 * }
 * }
 * }
 * <p>
 * joinfunction的两个输入时笛卡尔积的 左右两个元素
 * flink通过双层for循环实现两条流的join
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
