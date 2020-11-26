package com.gusi.flink.analysis.orderpay;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * OrderPayTimeoutDemo <br>
 * 订单支付超时检查
 *
 */
public class OrderPayTimeoutDemo {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		URL resource = env.getClass().getClassLoader().getResource("OrderLog.csv");
		DataStreamSource<String> orderLogString = env.readTextFile(resource.getPath());

		SingleOutputStreamOperator<OrderLog> orderLogStream = orderLogString.map(data -> {
			String[] line = data.split(",");
			return new OrderLog(Long.parseLong(line[0]), line[1], line[2], Long.parseLong(line[3]));
		}).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderLog>() {
			@Override
			public long extractAscendingTimestamp(OrderLog element) {
				return element.getTimestamp() * 1000L;
			}
		});

		//定义Pattern模型
		Pattern<OrderLog, OrderLog> pattern = Pattern
				.<OrderLog>begin("first").where(new IterativeCondition<OrderLog>() {
					@Override
					public boolean filter(OrderLog orderLog, Context<OrderLog> context) throws Exception {
						return orderLog.getType().equals("create");
					}
				})
				.next("second").where(new IterativeCondition<OrderLog>() {
					@Override
					public boolean filter(OrderLog orderLog, Context<OrderLog> context) throws Exception {
						return orderLog.getType().equals("pay");
					}
				}).within(Time.minutes(10));//10min超时时间


		// 流和模式进行匹配
		PatternStream<OrderLog> patternStream = CEP.pattern(orderLogStream.keyBy(key -> key.getOrderId()), pattern);


		// 提取匹配结果和超时结果，注意select有多个重构方法
		OutputTag<String> timeoutOut = new OutputTag<String>("timeoutOut") {
		};
		SingleOutputStreamOperator<Object> resultStream = patternStream.select(timeoutOut, //超时侧输出流，注意泛型需要和TimeoutFunction的第二个泛型一致
				new PatternTimeoutFunction<OrderLog, String>() { //超时处理函数
					@Override
					public String timeout(Map<String, List<OrderLog>> map, long l) throws Exception {
						long orderId = map.get("first").get(0).getOrderId();
						return String.format("order[%s] pay timeout until timestamp[%d]", orderId, l);
					}
				}, new PatternSelectFunction<OrderLog, Object>() { //正常提取函数
					@Override
					public Object select(Map<String, List<OrderLog>> map) throws Exception {
						OrderLog create = map.get("first").get(0);
						OrderLog payed = map.get("second").get(0);
						return String.format("order[%s] pay success, create[%d] payed[%d]", create.getOrderId(), create.getTimestamp(), payed.getTimestamp());
					}
				});


		resultStream.print("main");
		resultStream.getSideOutput(timeoutOut).print("side");

		env.execute("order pay timeout job");
	}
}
