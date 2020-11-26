package com.gusi.flink.analysis.login;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * AppLoginFailDemo <br>
 * 两分钟内fail次数判断
 *
 */
public class UserLoginFailDemo1 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStreamSource<String> sourceString = env.readTextFile("D:\\DevelopSpaces\\ideaSpace01\\demoFlink\\src\\main\\resources\\LoginLog.csv");

		SingleOutputStreamOperator<String> processStream = sourceString
				.map(data -> {
					String[] line = data.split(",");
					return new UserLoginLog(Long.parseLong(line[0]), line[1], line[2], Long.parseLong(line[3]));
				})
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserLoginLog>(Time.seconds(2)) {
					@Override
					public long extractTimestamp(UserLoginLog element) {
						return element.getTimestamp() * 1000;
					}
				})
				.keyBy(key -> key.getUserId())
				.process(new KeyedProcessFunction<Long, UserLoginLog, String>() {
					ListState<UserLoginLog> failList = null;


					@Override
					public void open(Configuration parameters) throws Exception {
						failList = getRuntimeContext().getListState(new ListStateDescriptor<UserLoginLog>("failList", UserLoginLog.class));
					}

					@Override
					public void processElement(UserLoginLog value, Context ctx, Collector<String> out) throws Exception {
						if (value.getResult().equals("fail")) {
							failList.add(value);
							//2s以后触发定时器
							ctx.timerService().registerEventTimeTimer(value.getTimestamp() * 1000 + 2000);
						} else {
							failList.clear();
						}

						//TODO:此处逻辑有漏洞，如果在2s内已经攻破密码，并且登录成功。对于这种情况是不能输出告警。
					}

					@Override
					public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
						List<UserLoginLog> logs = new ArrayList<>();
						failList.get().forEach((x) -> logs.add(x));

						if (logs.size() > 1) {
							out.collect(ctx.getCurrentKey() + " user login fail times is " + logs.size());
						}

						failList.clear();
					}

				});

		processStream.print();

		env.execute("login fail alert job");
	}
}
