package com.gusi.flink.analysis.login;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * AppLoginFailDemo <br>
 * 登录失败通过CEP检测
 *
 * 如果同一用户（可以是不同IP）在2秒之内连续两次登录失败，就认为存在恶意登录的风险，输出相关的信息进行报警提示。
 */
public class UserLoginFailDemo3 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStreamSource<String> sourceString = env.readTextFile("D:\\DevelopSpaces\\ideaSpace01\\demoFlink\\src\\main\\resources\\LoginLog.csv");
		SingleOutputStreamOperator<UserLoginLog> sourceStream = sourceString
				.map(data -> {
					String[] line = data.split(",");
					return new UserLoginLog(Long.parseLong(line[0]), line[1], line[2], Long.parseLong(line[3]));
				})
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserLoginLog>() {
					@Override
					public long extractAscendingTimestamp(UserLoginLog element) {
						return element.getTimestamp() * 1000;
					}
				});


		//定义Pattern模型
		Pattern<UserLoginLog, UserLoginLog> pattern = Pattern.<UserLoginLog>begin("first")
				.where(new IterativeCondition<UserLoginLog>() {
					@Override
					public boolean filter(UserLoginLog userLoginLog, Context<UserLoginLog> context) throws Exception {
						return userLoginLog.getResult().equals("fail");
					}
				}).next("second").where(new IterativeCondition<UserLoginLog>() {
					@Override
					public boolean filter(UserLoginLog userLoginLog, Context<UserLoginLog> context) throws Exception {
						return userLoginLog.getResult().equals("fail");
					}
				}).within(Time.seconds(2));//设置模型内元素的时间区间


		// 流和模式进行匹配
		PatternStream<UserLoginLog> patternStream = CEP.pattern(sourceStream.keyBy(key -> key.getUserId()), pattern);


		// 匹配结果进行提取
		SingleOutputStreamOperator<String> resultStream1 = patternStream.select(new PatternSelectFunction<UserLoginLog, String>() {
			@Override
			public String select(Map<String, List<UserLoginLog>> map) throws Exception {
				UserLoginLog first = map.get("first").get(0);
				UserLoginLog second = map.get("second").get(0);
				return String.format("user %s login fail first %d second %d", first.getUserId(), first.getTimestamp(), second.getTimestamp());
			}
		});


		// 如果使用flatSelect，可以提取多个结果
		SingleOutputStreamOperator<UserLoginLog> resultStream2 = patternStream.flatSelect(new PatternFlatSelectFunction<UserLoginLog, UserLoginLog>() {
			OutputTag<UserLoginLog> sideStream= new OutputTag("lateLog");
			@Override
			public void flatSelect(Map<String, List<UserLoginLog>> map, Collector<UserLoginLog> collector) throws
					Exception {

				UserLoginLog first = map.get("first").get(0);
				UserLoginLog second = map.get("second").get(0);
				collector.collect(first);
				collector.collect(second);
			}
		});


		resultStream2.print();

		DataStream<UserLoginLog> lateLog = resultStream2.getSideOutput(new OutputTag<UserLoginLog>("lateLog"));
		lateLog.print("later");

		env.execute("login fail alert job");
	}
}
