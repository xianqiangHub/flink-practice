package com.gusi.flink.analysis.goodsStatistic;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
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
import java.util.concurrent.TimeUnit;

/**
 * StreamUrlStatisticDemo <br>
 *     热门页面统计
 */
public class StreamUrlStatisticDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //设置时间语义为事件发生时间


		DataStreamSource<String> sourceString = env.readTextFile("D:\\DevelopSpaces\\ideaSpace01\\demoFlink\\src\\main\\resources\\apache.log");
		SingleOutputStreamOperator<ApacheLog> sourceStream = sourceString.map((s) -> {
			String[] line = s.split(" ");
			ApacheLog log = new ApacheLog();
			log.setIp(line[0]);
			SimpleDateFormat f = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
			log.setTimestamp(f.parse(line[3]).getTime());
			log.setMethod(line[5]);
			log.setUrl(line[6]);
			return log;
		}).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLog>(Time.seconds(1)) {
			@Override
			public long extractTimestamp(ApacheLog apacheLog) {
				return apacheLog.getTimestamp();
			}
		});//指定事件时间的字段，设置watermark的延迟为1秒。

		SingleOutputStreamOperator<String> processStream = sourceStream.filter(new FilterFunction<ApacheLog>() { //过滤css和js
			String pattern = "^((?!\\.(css|js)$).)*$";

			@Override
			public boolean filter(ApacheLog apacheLog) throws Exception {
				return apacheLog.getUrl().matches(pattern);
			}
		}).keyBy(new KeySelector<ApacheLog, String>() { //按照url分类
			@Override
			public String getKey(ApacheLog apacheLog) throws Exception {
				return apacheLog.getUrl();
			}
		}).timeWindow(Time.minutes(1), Time.seconds(10)).aggregate(new AggregateFunction<ApacheLog, Long, Long>() { //开窗后聚合
			@Override
			public Long createAccumulator() {
				return 0L;
			}

			@Override
			public Long add(ApacheLog apacheLog, Long aLong) {
				return aLong + 1;
			}

			@Override
			public Long getResult(Long aLong) {
				return aLong;
			}

			@Override
			public Long merge(Long aLong, Long acc1) {
				return acc1 + aLong;
			}
		}, new WindowFunction<Long, UrlStatistic, String, TimeWindow>() {
			@Override
			public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<UrlStatistic> out) throws Exception {
				out.collect(new UrlStatistic(key, input.iterator().next().longValue(), window.getStart(), window.getEnd()));
//				out.collect(String.format("time:%d~%d#url:%key;count:%key", window.getStart(), window.getEnd(), key, input.iterator().next()));
			}
		}).keyBy((urlStatistic) -> { //再以窗口结束时间分类
			return urlStatistic.getEndTime();
		}).process(new KeyedProcessFunction<Long, UrlStatistic, String>() {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			ListState<UrlStatistic> listState = null;

			@Override
			public void open(Configuration parameters) throws Exception {
				listState = getRuntimeContext().getListState(new ListStateDescriptor<UrlStatistic>("listUrlCount", UrlStatistic.class));
			}

			@Override
			public void processElement(UrlStatistic value, Context ctx, Collector<String> out) throws Exception {
				listState.add(value);
				ctx.timerService().registerEventTimeTimer(value.getEndTime() + 1);//在窗口结束时触发onTimer
			}

			@Override
			public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
				//窗口结束时触发
				List<UrlStatistic> temp = new ArrayList<>();
				listState.get().forEach((x) -> {
					temp.add(x);
				});

				listState.clear();

				temp.sort(new Comparator<UrlStatistic>() {
					@Override
					public int compare(UrlStatistic o1, UrlStatistic o2) {
						return (int) (o2.getCount() - o1.getCount());
					}
				});


				StringBuffer result = new StringBuffer();
				result.append("时间：").append(format.format(new Date(temp.get(0).getStartTime())))
						.append("~").append(format.format(new Date(timestamp - 1))).append("\n");

				int len = temp.size() > 3 ? 3 : temp.size();
				for (int i = 0; i < len; i++) {
					result.append("No.").append(i + 1).append("#url:").append(temp.get(i).getUrl()).append("#").append(temp.get(i).getCount()).append("\n");
				}
				out.collect(result.toString());

				TimeUnit.SECONDS.sleep(1);
			}
		});

		sourceStream.print("sourceStream");
		processStream.print("processStream");

		env.execute("topn url job");
	}
}
