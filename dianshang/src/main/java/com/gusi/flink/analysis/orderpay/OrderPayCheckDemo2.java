package com.gusi.flink.analysis.orderpay;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;

/**
 * OrderPayCheckDemo2 <br>
 * 两条流join处理
 * intervalJoin
 */
public class OrderPayCheckDemo2 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		URL resource1 = env.getClass().getClassLoader().getResource("ReceiptLog.csv");
		DataStreamSource<String> sourcePayedRspString = env.readTextFile(resource1.getPath());

		URL resource2 = env.getClass().getClassLoader().getResource("OrderLog.csv");
		DataStreamSource<String> sourcePayedLogString = env.readTextFile(resource2.getPath());

		KeyedStream<OrderLog, String> payedLogStream = sourcePayedLogString.map(data -> {
			String[] line = data.split(",");
			return new OrderLog(Long.parseLong(line[0]), line[1], line[2], Long.parseLong(line[3]));
		}).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderLog>() {
			@Override
			public long extractAscendingTimestamp(OrderLog element) {
				return element.getTimestamp() * 1000L;
			}
		}).filter(data -> !data.getTxId().trim().equals("")).keyBy(key -> key.getTxId());

		KeyedStream<PayedRsp, String> payedRspStream = sourcePayedRspString.map(data -> {
			String[] line = data.split(",");
			return new PayedRsp(line[0], line[1], Long.parseLong(line[2]));
		}).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<PayedRsp>() {
			@Override
			public long extractAscendingTimestamp(PayedRsp element) {
				return element.getTimestamp() * 1000L;
			}
		}).keyBy(key -> key.getTxId());

		//join合并处理两条流
		SingleOutputStreamOperator<String> resultStream = payedLogStream.intervalJoin(payedRspStream).between(Time.seconds(-50), Time.seconds(50)).process(new ProcessJoinFunction<OrderLog, PayedRsp, String>() {
			@Override
			public void processElement(OrderLog left, PayedRsp right, Context ctx, Collector<String> out) throws Exception {
				out.collect(String.format("match payedlog@payedrsp-> ") + left.toString() + "@" + right.toString());
			}
		});

		//TODO:两个问题需要注意
		//1、只能筛选出匹配的（符合要求）的流对象，不能获取不符合要求的
		//2、事件时间和watermark就没关系了，只能单纯的判断eventtime

		payedLogStream.print("log");
		payedRspStream.print("rsp");
		resultStream.print("main");

		env.execute("stream join job");


	}
}
