package com.gusi.flink.simple.apitest;

import com.gusi.flink.simple.StreamDTO;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * StreamValueChangeProcess <br>
 *
 */
public class StreamValueChangeProcess extends KeyedProcessFunction<Tuple, StreamDTO, String> {
	private double threshold;

	public StreamValueChangeProcess(double threshold) {
		this.threshold = threshold;
	}

	ValueState<Double> preValue = null;

	@Override
	public void open(Configuration parameters) throws Exception {
		System.out.println("初始化open..."); //注意，每个并行任务都会执行此处
		preValue = getRuntimeContext().getState(new ValueStateDescriptor<Double>("preValue", Double.class));
	}


	@Override
	public void processElement(StreamDTO streamDto, Context context, Collector<String> collector) throws Exception {
		if (preValue.value() == null || preValue.value().doubleValue() == 0) {
			//nothing to do.
		} else {
			boolean changed = Math.abs(preValue.value().doubleValue() - streamDto.getValue()) > this.threshold;
			if (changed) {
				collector.collect("value跳变告警：" + streamDto.getDtoId() + "#" + preValue.value() + "->" + streamDto.getValue());
			}
		}

		preValue.update(streamDto.getValue());
	}
}
