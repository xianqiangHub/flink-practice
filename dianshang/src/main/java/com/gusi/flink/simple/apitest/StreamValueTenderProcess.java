package com.gusi.flink.simple.apitest;

import com.gusi.flink.simple.StreamDTO;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


public class StreamValueTenderProcess extends KeyedProcessFunction<String, StreamDTO, String> {
	ValueState<Long> timerSt = null;
	ValueState<Double> lastValue = null;

	@Override
	public void open(Configuration parameters) throws Exception {
		timerSt = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerSt", Long.class));
		lastValue = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastValue", Double.class));
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
		out.collect(ctx.getCurrentKey() + "#value连续上升告警！");
		timerSt.clear();
	}

	@Override
	public void processElement(StreamDTO streamDto, Context context, Collector<String> collector) throws Exception {
		double perValue = lastValue.value() == null ? 0 : lastValue.value();
		lastValue.update(streamDto.getValue());

		long currentSt = timerSt.value() == null ? 0 : timerSt.value();

		if (perValue > streamDto.getValue() || perValue == 0) {
			//第一次或减小value,
			context.timerService().deleteProcessingTimeTimer(currentSt);
			timerSt.clear();
		} else if (perValue < streamDto.getValue() && currentSt == 0) {
			//value持续增大,注册处理器
			long nextSt = context.timerService().currentProcessingTime() + 10000L;
			System.out.println(nextSt);
			context.timerService().registerProcessingTimeTimer(nextSt);

			timerSt.update(nextSt);
		} else {
			System.out.println("已经注册了timer,无需重复注册。");
		}
	}
}


