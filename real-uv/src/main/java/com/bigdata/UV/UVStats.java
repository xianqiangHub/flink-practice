package com.bigdata.UV;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 可以借鉴 windowfunction
 * 这个现在的逻辑是每10分钟计算一次UV，对状态加了过期时间
 */
public class UVStats {

    public static final String DELIM = "\t";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(1000L);

        //For RocksDBStateBackend

        /*RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs://hostname:8020/yourDir/checkpoint", true);
        rocksDBStateBackend.enableTtlCompactionFilter();
        env.setStateBackend(rocksDBStateBackend);*/

        env.addSource(new LogSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
                    public long extractAscendingTimestamp(String s) {
                        String dateTime = s.split(DELIM, -1)[2];
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        return LocalDateTime.parse(dateTime, formatter).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                    }
                })
                .keyBy(new KeySelector<String, String>() {
                    public String getKey(String s) {
                        String url = s.split(DELIM, -1)[0];
                        return url;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .process(new UVWindowFunction())
                .print();

        env.execute();
    }
}
