package com.gusi.flink.analysis.goodsStatistic;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

/**
 * StreamGoodsStatisticDemo <br>
 * 热门商品统计
 *
 */
public class StreamGoodsStatisticDemo {
	public static void main(String[] args) throws Exception {
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

		SingleOutputStreamOperator<String> resultStream = sourceStream.filter((data) -> {
			return data.getBehave().equals("pv");
		}).keyBy((k) -> {
			return k.getGoodsId();
		}).timeWindow(Time.minutes(60), Time.minutes(10)).aggregate(new AggregateFunction<UserBehave, Long, Long>() {
			@Override
			public Long createAccumulator() {
				return 0L;
			}

			@Override
			public Long add(UserBehave value, Long accumulator) {
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
		}, new WindowFunction<Long, GoodsStatistic, Long, TimeWindow>() {
			@Override
			public void apply(Long key, TimeWindow window, Iterable<Long> input, Collector<GoodsStatistic> out) throws Exception {
				out.collect(new GoodsStatistic(key, input.iterator().next(), window.getStart(), window.getEnd()));
			}
		}).keyBy(k -> k.getEndTime()).process(new KeyedProcessFunction<Long, GoodsStatistic, String>() {
			ListState<GoodsStatistic> listState = null;
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

			@Override
			public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
				List<GoodsStatistic> list = new ArrayList<>();
				listState.get().forEach(x -> {
					list.add(x);
				});

				listState.clear();

				list.sort(new Comparator<GoodsStatistic>() {
					@Override
					public int compare(GoodsStatistic o1, GoodsStatistic o2) {
						return (int) (o2.getCount() - o1.getCount());
					}
				});
				StringBuffer sb = new StringBuffer();
				sb.append(format.format(new Date(list.get(0).getStartTime())))
						.append("~").append(format.format(new Date(timestamp - 1)))
						.append("=").append(format.format(new Date(ctx.getCurrentKey()))).append("\n");
				for (int i = 0; i < 2; i++) {
					sb.append("No.").append(i + 1).append("# goodsid=").append(list.get(i).getGoodsId()).append(" count=").append(list.get(i).getCount()).append("\n");

				}
				out.collect(sb.toString());
			}

			@Override
			public void open(Configuration parameters) throws Exception {
				listState = getRuntimeContext().getListState(new ListStateDescriptor<GoodsStatistic>("listState", GoodsStatistic.class));
			}

			@Override
			public void processElement(GoodsStatistic value, Context ctx, Collector<String> out) throws Exception {
				listState.add(value);
				ctx.timerService().registerEventTimeTimer(value.getEndTime() + 1);
			}
		});

		resultStream.print("result");

		env.execute("topn goods job");
	}
}
