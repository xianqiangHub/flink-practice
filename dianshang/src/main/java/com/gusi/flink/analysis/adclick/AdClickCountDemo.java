package com.gusi.flink.analysis.adclick;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * AdClickCountDemo <br>
 * 广告点击统计
 *
 */
public class AdClickCountDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		URL resource = env.getClass().getClassLoader().getResource("AdClickLog.csv");
		DataStreamSource<String> sourceString = env.readTextFile(resource.getPath());

		SingleOutputStreamOperator<String> resultStream = sourceString
				.map(data -> {
					String[] line = data.split(",");
					return new AdClick(Long.parseLong(line[0]), Long.parseLong(line[1]), line[2], line[3], Long.parseLong(line[4]));
				}).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClick>() {
					@Override
					public long extractAscendingTimestamp(AdClick element) {
						return element.getTimestamp() * 1000;
					}
				})
				.keyBy(key -> key.getProvince())
				.timeWindow(Time.minutes(60), Time.minutes(10))
				.aggregate(new AggregateFunction<AdClick, Long, Long>() {
					@Override
					public Long createAccumulator() {
						return 0L;
					}

					@Override
					public Long add(AdClick value, Long accumulator) {
						return accumulator + 1;
					}

					@Override
					public Long getResult(Long accumulator) {
						return accumulator;
					}

					@Override
					public Long merge(Long a, Long b) {
						return a + b;
					}
				}, new ProcessWindowFunction<Long, String, String, TimeWindow>() {
					@Override
					public void process(String key, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
						SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						//注意此处的window的start和end是flink的内部管理时间，不需要*1000L
						out.collect(String.format("area[%s] in time[%s~%s] click ad times[%d]", key, format.format(new Date(context.window().getStart())), format.format(new Date(context.window().getEnd())), elements.iterator().next().longValue()));
					}
				});

		resultStream.print("main");
		env.execute("ad click job");
	}
}
