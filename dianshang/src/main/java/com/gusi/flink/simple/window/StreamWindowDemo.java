package com.gusi.flink.simple.window;

import com.gusi.flink.simple.StreamDTO;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * StreamWindowDemo <br>
 *
 */
public class StreamWindowDemo {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStreamSource<String> sourceString = env.socketTextStream("localhost", 9000);

		DataStream<StreamDTO> sourceStream = sourceString
				.map((s) -> {
					String[] line = s.split(" ");
					return new StreamDTO(line[0], Double.parseDouble(line[1]), Long.parseLong(line[2]));
				})
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<StreamDTO>() { //设置使用dto的时间戳为eventtime
					@Override
					public long extractAscendingTimestamp(StreamDTO element) {
						return element.getTimestamp();
					}
				});

		SingleOutputStreamOperator<String> resultStream = sourceStream
				.keyBy((dto) -> dto.getDtoId())
				.timeWindow(Time.seconds(10), Time.seconds(5))  //开窗（滑动窗口）
				.apply(new WindowFunction<StreamDTO, String, String, TimeWindow>() {
					//使用dto的eventtime作为开窗时间的判断依据，current.dto.timestamp-current.window.start>100000时关闭一个窗口。
					@Override
					public void apply(String s, TimeWindow window, Iterable<StreamDTO> input, Collector<String> out) throws Exception {
						StringBuffer sb = new StringBuffer();
						for (StreamDTO dto : input) {
							sb.append(dto.getValue()).append(";");
						}
						out.collect(String.format("key for [%s] in window time [%d~%d), out values [%s]", s, window.getStart(), window.getEnd(), sb.toString()));
					}
				});

		sourceStream.print("source");
		resultStream.print("result");


		try {
			env.execute("window job");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
