package com.gusi.flink.analysis.login;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;

/**
 * AppLoginFailDemo <br>
 * 连续两次失败告警
 *
 */
public class UserLoginFailDemo2 {
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
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserLoginLog>() {
					@Override
					public long extractAscendingTimestamp(UserLoginLog element) {
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
							if (failList.get().iterator().hasNext()) {
								UserLoginLog last = failList.get().iterator().next();
								if (value.getTimestamp() - last.getTimestamp() < 2) {
									//两次失败间隔小于2s
									out.collect(String.format("user[%s] login fail times 2.", value.getUserId()));
								}
								failList.clear();
							}
							failList.add(value);
						} else {
							failList.clear();
						}
					}

					//TODO:此种方案依然有问题
					//1、如果要求改为连续N次检测，代码逻辑改动比较大。
					//2、如果数据来的顺序是乱序的，逻辑就不符合了。
					//TODO：引入watermark，CEP
				});

		processStream.print();

		env.execute("login fail alert job");
	}
}
