package com.gusi.flink.simple.window;

import com.gusi.flink.simple.StreamDTO;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * StreamLaterOutputDemo <br>
 * 迟到数据输出到侧输出流
 *
 */
public class StreamLaterOutputDemo {
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
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<StreamDTO>(Time.milliseconds(1000)) {
					//设置使用dto的时间戳为eventtime，并且设置watermark为1秒。需要注意watermark的临界值和window的区间的end的值的包含问题。
					@Override
					public long extractTimestamp(StreamDTO element) {
						return element.getTimestamp() * 1000;
					}
				});

		OutputTag<StreamDTO> laterOut = new OutputTag<StreamDTO>("laterOut") {
		};//注意这个{}
		SingleOutputStreamOperator<String> resultStream = sourceStream
				.keyBy(key -> key.getDtoId())
				.timeWindow(Time.seconds(10)) //开窗
				.allowedLateness(Time.seconds(1)) //允许数据迟到一秒
				.sideOutputLateData(laterOut) //迟到数据(超出允许迟到时间范围)保存在侧输出流，如果没有sideout就会丢弃。
				.apply(new WindowFunction<StreamDTO, String, String, TimeWindow>() {
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
		resultStream.getSideOutput(laterOut).print("side");


		try {
			env.execute("window job");
		} catch (
				Exception e) {
			e.printStackTrace();
		}
	}
}
