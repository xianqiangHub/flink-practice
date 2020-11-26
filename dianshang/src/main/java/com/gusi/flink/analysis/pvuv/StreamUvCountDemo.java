package com.gusi.flink.analysis.pvuv;

import com.gusi.flink.analysis.goodsStatistic.UserBehave;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

/**
 * StreamUvCountDemo <br>
 * uv 统计
 *
 */
public class StreamUvCountDemo {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStreamSource<String> sourceString = env.readTextFile("D:\\DevelopSpaces\\ideaSpace01\\demoFlink\\src\\main\\resources\\UserBehavior.csv");

		SingleOutputStreamOperator<UserBehave> sourceStream = sourceString.map((data) -> {
			String[] line = data.split(",");
			return new UserBehave(Long.valueOf(line[0]), Long.valueOf(line[1]), Integer.valueOf(line[2]), line[3], Long.valueOf(line[4]));
		}).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserBehave>(Time.seconds(1)) {
			@Override
			public long extractTimestamp(UserBehave element) {
				return element.getTimestamp() * 1000L;
			}
		});

		DataStream<CountResult> resultStream = sourceStream.filter((data) -> {
			return data.getBehave().equals("pv");
		}).timeWindowAll(Time.minutes(60)).apply(new AllWindowFunction<UserBehave, CountResult, TimeWindow>() {
			Set<Long> uvSet = new HashSet<>(); //此处有内存溢出风险，因为所以数据都暂存在内存中

			@Override
			public void apply(TimeWindow window, Iterable<UserBehave> values, Collector<CountResult> out) throws Exception {
				values.forEach(new Consumer<UserBehave>() {
					@Override
					public void accept(UserBehave count) {
						uvSet.add(count.getUserId());
					}
				});

				out.collect(new CountResult(window.getEnd(), values.iterator().next().getBehave(), uvSet.size()));
				uvSet.clear();
			}
		});

		resultStream.print("result");

		try {
			env.execute("uv count job");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
