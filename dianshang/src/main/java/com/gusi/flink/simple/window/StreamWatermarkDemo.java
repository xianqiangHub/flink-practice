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
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * StreamWindowDemo <br>
 *
 */
public class StreamWatermarkDemo {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStreamSource<String> sourceString = env.socketTextStream("localhost", 9000);

		DataStream<StreamDTO> sourceStream = sourceString
				.map((s) -> {
					String[] line = s.split(" ");
					return new StreamDTO(line[0], Double.valueOf(line[1]), Long.valueOf(line[2]));
				})
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<StreamDTO>(Time.milliseconds(1000)) {
					//设置使用dto的时间戳为eventtime，并且设置watermark为1秒。需要注意watermark的临界值和window的区间的end的值的包含问题。
					@Override
					public long extractTimestamp(StreamDTO element) {
						return element.getTimestamp();
					}
				});

		//使用dto的eventtime作为开窗时间的判断依据，current.dto.timestamp-current.window.start>100000时关闭一个窗口。
		SingleOutputStreamOperator<String> resultStream = sourceStream
				.keyBy((dto) -> dto.getDtoId())
				.timeWindow(Time.seconds(10)) //开窗
				.apply((WindowFunction<StreamDTO, String, String, TimeWindow>) (s, window, input, out) -> {
					StringBuffer sb = new StringBuffer();
					for (StreamDTO dto : input) {
						sb.append(dto.getValue()).append(";");
					}
					out.collect(String.format("key for [%s] in window time [%d~%d), out values [%s]", s, window.getStart(), window.getEnd(), sb.toString()));
				});

		sourceStream.print("source");
		resultStream.print("result");


		try {
			env.execute("window watermark job");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
