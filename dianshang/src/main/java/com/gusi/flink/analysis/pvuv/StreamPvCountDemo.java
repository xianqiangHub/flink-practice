package com.gusi.flink.analysis.pvuv;

import com.gusi.flink.analysis.goodsStatistic.UserBehave;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * StreamPvCountDemo <br>
 * pv统计
 */
public class StreamPvCountDemo {
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

		DataStream resultStream = sourceStream.filter((data) -> {
			return data.getBehave().equals("pv");
		}).map(new MapFunction<UserBehave, Count>() { //为了统计，再次对象转换
			@Override
			public Count map(UserBehave value) throws Exception {
				return new Count("pv", 1L);
			}
		}).timeWindowAll(Time.minutes(60), Time.minutes(10)).sum("value");
		//timeWindowAll和timeWindow的区别？前者处理非keyed的流，后者处理keyed的流

		resultStream.print("result");

		try {
			env.execute("pv count job");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
