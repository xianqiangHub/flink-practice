package com.gusi.flink.analysis.pvuv;

import com.gusi.flink.analysis.goodsStatistic.UserBehave;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * StreamUvCountDemo2 <br>
 * uv统计，使用布隆过滤器
 *
 */
public class StreamUvCountDemo2 {
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
		}).map(new MapFunction<UserBehave, Count>() {
			@Override
			public Count map(UserBehave value) throws Exception {
				return new Count("dumpKey", value.getUserId());
			}
		}).keyBy(k -> k.getType())
				.timeWindow(Time.minutes(60))
				.trigger(new Trigger<Count, TimeWindow>() { //每条数据都立即处理，不在（窗口内）内存中逗留。
					@Override
					public TriggerResult onElement(Count element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
						// 每来一条数据，就触发窗口操作并清空
						return TriggerResult.FIRE_AND_PURGE;
					}

					@Override
					public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
						return TriggerResult.CONTINUE;
					}

					@Override
					public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
						return TriggerResult.CONTINUE;
					}

					@Override
					public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
						//因为已经在事件处理过程时清理了，所以此处不许再做任何处理
					}
				}).process(new ProcessWindowFunction<Count, CountResult, String, TimeWindow>() {
					BloomCache2 bloom = null;

					@Override
					public void open(Configuration parameters) throws Exception {
						bloom = new BloomCache2();
					}

					@Override
					public void process(String key, Context context, Iterable<Count> elements, Collector<CountResult> out) throws Exception {
						long offset = bloom.hash(elements.iterator().next().getValue() + "", 61);

						boolean flag = BloomCache.getInstance().putValue(context.window().getEnd(), elements.iterator().next().getValue() + "");

						// flag=false:旧数据
						if (flag) { //不包含的数据才out
							out.collect(new CountResult(context.window().getEnd(), "uv", BloomCache.countMap.get(context.window().getEnd())));
						}
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
