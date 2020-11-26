package com.gusi.flink.analysis.orderpay;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 * OrderPayCheckDemo <br>
 * 两条流合并处理
 */
public class OrderPayCheckDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		URL resource1 = env.getClass().getClassLoader().getResource("ReceiptLog.csv");
		DataStreamSource<String> sourcePayedRspString = env.readTextFile(resource1.getPath());

		URL resource2 = env.getClass().getClassLoader().getResource("OrderLog.csv");
		DataStreamSource<String> sourcePayedLogString = env.readTextFile(resource2.getPath());

		SingleOutputStreamOperator<OrderLog> payedLogStream = sourcePayedLogString.map(data -> {
			String[] line = data.split(",");
			return new OrderLog(Long.parseLong(line[0]), line[1], line[2], Long.parseLong(line[3]));
		}).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderLog>() {
			@Override
			public long extractAscendingTimestamp(OrderLog element) {
				return element.getTimestamp() * 1000L;
			}
		});

		SingleOutputStreamOperator<PayedRsp> payedRspStream = sourcePayedRspString.map(data -> {
			String[] line = data.split(",");
			return new PayedRsp(line[0], line[1], Long.parseLong(line[2]));
		}).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<PayedRsp>() {
			@Override
			public long extractAscendingTimestamp(PayedRsp element) {
				return element.getTimestamp() * 1000L;
			}
		});


		//主流输出正常匹配的
		//两个侧流输出未匹配的
		OutputTag<PayedRsp> unpaylogOut = new OutputTag<>("unpaylogOut", TypeInformation.of(PayedRsp.class));
		OutputTag<OrderLog> unpayrspOut = new OutputTag<OrderLog>("unpayrspOut"){};

		SingleOutputStreamOperator<Object> resultStream = payedLogStream
				.filter(data -> !data.getTxId().equals(""))
				.keyBy(key -> key.getTxId())
				.connect(payedRspStream.keyBy(key -> key.getTxId()))
				.process(new CoProcessFunction<OrderLog, PayedRsp, Object>() {
					ValueState<OrderLog> payedLogState = null;
					ValueState<PayedRsp> payedRspState = null;

					@Override
					public void open(Configuration parameters) throws Exception {
						payedLogState = getRuntimeContext().getState(new ValueStateDescriptor<OrderLog>("payedlogState", OrderLog.class));
						payedRspState = getRuntimeContext().getState(new ValueStateDescriptor<PayedRsp>("payedrspState", PayedRsp.class));
					}

					//支付记录流
					@Override
					public void processElement1(OrderLog value, Context ctx, Collector<Object> out) throws Exception {
						PayedRsp payedRsp = payedRspState.value();
						if (payedRsp != null) {
							// 有支付响应
							out.collect(result(0, value, payedRsp));
							payedRspState.clear();
						} else {
							// 没有支付响应，但有支付记录
							payedLogState.update(value);
							ctx.timerService().registerEventTimeTimer(value.getTimestamp() * 1000 + 5000L);
						}
					}

					//支付响应流
					@Override
					public void processElement2(PayedRsp value, Context ctx, Collector<Object> out) throws Exception {
						OrderLog payedLog = payedLogState.value();
						if (payedLog != null) {
							// 有支付记录
							out.collect(result(0, payedLog, value));
							payedLogState.clear();
						} else {
							// 没有支付记录，但有支付响应
							payedRspState.update(value);
							ctx.timerService().registerEventTimeTimer(value.getTimestamp() * 1000 + 5000L);
						}

					}

					@Override
					public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
						if (payedLogState.value() != null) {
							//没等到支付响应流
							ctx.output(unpayrspOut, payedLogState.value());
							out.collect(result(1, payedLogState.value(), payedRspState.value()));
						}

						if (payedRspState.value() != null) {
							//没等到支付记录流
							ctx.output(unpaylogOut, payedRspState.value());
							out.collect(result(2, payedLogState.value(), payedRspState.value()));
						}

						payedLogState.clear();
						payedRspState.clear();
					}
				});

		resultStream.print("main");
		resultStream.getSideOutput(unpaylogOut).print("side1");
		resultStream.getSideOutput(unpayrspOut).print("side2");

		env.execute("order pay check job");
	}


	private static String result(int matched, OrderLog payedLog, PayedRsp payedRsp) {
		if (matched == 0) {
			return "matched all->" + payedLog.toString() + "@" + payedRsp.toString();
		} else if (matched == 1) {
			return "unmatch rsp->" + payedLog.toString() + "@" + null;
		} else if (matched == 2) {
			return "unmatch log->" + null + "@" + payedRsp.toString();
		}
		return null;
	}
}
