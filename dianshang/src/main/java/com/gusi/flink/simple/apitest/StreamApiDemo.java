package com.gusi.flink.simple.apitest;

import com.gusi.flink.simple.StreamDTO;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * StreamApiDemo <br>
 *
 */
public class StreamApiDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
		DataStream<String> sourceStream = env.socketTextStream("localhost", 9000, "\n");

		DataStream<StreamDTO> stream1 = sourceStream.flatMap(new FlatMapFunction<String, StreamDTO>() {
			@Override
			public void flatMap(String s, Collector<StreamDTO> collector) throws Exception {
				String[] s1 = s.split(" ");
				collector.collect(new StreamDTO(s1[0], Double.valueOf(s1[1]), Long.valueOf(s1[2])));
			}
		});

		KeyedStream<StreamDTO, Tuple> keyedStream = stream1.keyBy("dtoId");

		KeyedStream<StreamDTO, String> keyedStream1 = stream1.keyBy(new KeySelector<StreamDTO, String>() {
			@Override
			public String getKey(StreamDTO streamDto) throws Exception {
				return streamDto.getDtoId();
			}
		});
		DataStream<StreamDTO> stream2 = keyedStream1.reduce(new ReduceFunction<StreamDTO>() {
			@Override
			public StreamDTO reduce(StreamDTO streamDto, StreamDTO t1) throws Exception {
				return new StreamDTO(streamDto.getDtoId(), streamDto.getValue() + t1.getValue(), t1.getTimestamp());
			}
		});


		SingleOutputStreamOperator<String> stream3 = keyedStream.process(new StreamValueChangeProcess(10));

		SingleOutputStreamOperator<String> stream4 = keyedStream1.process(new StreamValueTenderProcess());

		stream1.print("s1");
		stream2.print("s2");
		stream3.print("s3");
		stream4.print("s4");

		env.execute("api test demo");
	}
}


