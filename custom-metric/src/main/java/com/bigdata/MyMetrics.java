package com.bigdata;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 一种自己定义反序列化方式，在其中加入的指标统计，如下：
 * https://mp.weixin.qq.com/s?__biz=MzI0NTIxNzE1Ng==&mid=2651218312&idx=1&sn=edfa0a4ff115d8f84bc978c38ebda9dd&chksm=f2a32363c5d4aa757e2f5642a0caaa6d2be83bdbf02fa005e22ef034f4e24cb4ffe12cf18cc3&mpshare=1&scene=1&srcid=&sharer_sharetime=1572788370267&sharer_shareid=0468516597d0c1eb4dc5374de67f4ed5&pass_ticket=%2F2wb2c5501xELKeMZOAnufAh5zbj5Cvthk0qzY0kzfpGG3yPiVHYH3Ubm8Q%2BUdV9#rd
 * <p>
 * metric监控源码分析：https://blog.csdn.net/qq_21653785/article/details/79625601
 *
 * 通过restful API获取指标
 * https://blog.csdn.net/aA518189/article/details/88952910
 */
public class MyMetrics {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("topic", new SimpleStringSchema(), props);
        DataStreamSource<String> stream = env.addSource(consumer);

        stream.map(new RichMapFunction<String, String>() {

            Counter failedCounter;
            Counter successCounter;

            @Override
            public void open(Configuration parameters) throws Exception {
                //metric管理metric
                failedCounter = getRuntimeContext().getMetricGroup().addGroup("countgroup").counter("failedjson");
                successCounter = getRuntimeContext().getMetricGroup().addGroup("countgroup").counter("successjson");
            }

            @Override
            public String map(String value) throws Exception {

                //成功的
                successCounter.inc();
                //失败的  catch
                failedCounter.inc();
                return null;
            }
        }).print();

        env.execute("excutor");
    }
}
