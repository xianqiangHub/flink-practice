package com.gusi.flink.analysis.adclick;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * AdClickCountDemo <br>
 * 广告点击统计，带黑名单
 *
 */
public class AdClickCountDemo2 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		URL resource = env.getClass().getClassLoader().getResource("AdClickLog.csv");
		DataStreamSource<String> sourceString = env.readTextFile(resource.getPath());


		//黑名单输出流
		OutputTag<String> blackListOut = new OutputTag<>("blackListOut", TypeInformation.of(String.class));

		//对用户点击进行过滤后再输出到下一个流
		SingleOutputStreamOperator<AdClick> blackFilterStream = sourceString
				.map(data -> {
					String[] line = data.split(",");
					return new AdClick(Long.parseLong(line[0]), Long.parseLong(line[1]), line[2], line[3], Long.parseLong(line[4]));
				})
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClick>() {
					@Override
					public long extractAscendingTimestamp(AdClick element) {
						return element.getTimestamp() * 1000;
					}
				})
				.keyBy("userId", "adId") //按照userId和adId过滤
				.process(new KeyedProcessFunction<Tuple, AdClick, AdClick>() {
					ValueState<Long> clickCount = null; //点击次数
					ValueState<Long> timerTs = null; //注册定时器触发事件

					Set<String> alreadySendBlack = new HashSet<>();// 已经发送到黑名单的不再发送，也可以用ValueState<Boolean>来标记是否已经记录

					@Override
					public void open(Configuration parameters) throws Exception {
						clickCount = getRuntimeContext().getState(new ValueStateDescriptor<Long>("clickCount", Long.class));
						timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
					}

					@Override
					public void processElement(AdClick value, Context ctx, Collector<AdClick> out) throws Exception {
						if (clickCount.value() == null || clickCount.value() == 0) {
							clickCount.update(1L);

							//如果第一次统计，注册一个定时器再第二天清零统计
							long ts = (ctx.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000);
							ctx.timerService().registerProcessingTimeTimer(ts);//注意此处是ProcessingTime

							timerTs.update(ts);
						} else {
							clickCount.update(clickCount.value() + 1);
						}

						if (clickCount.value() > 100) {
							//发送的黑名单的事件，不再向out的后面流动
							if (!alreadySendBlack.contains(value.getUserId() + "@" + value.getAdId())) {
								ctx.output(blackListOut, String.format("user[%d] click ad[%d] times > 100, add to blacklist!!!", value.getUserId(), value.getAdId()));
								alreadySendBlack.add(value.getUserId() + "@" + value.getAdId());
							}

							return;
						}

						out.collect(value);
					}

					@Override
					public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClick> out) throws Exception {
						if (timestamp == timerTs.value()) {
							clickCount.clear();
							alreadySendBlack.clear();
						}
					}
				});


		SingleOutputStreamOperator<String> resultStream = blackFilterStream
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

		blackFilterStream.getSideOutput(blackListOut).print("blackList");
		resultStream.print("main");

		env.execute("ad click with black list job");
	}
}
