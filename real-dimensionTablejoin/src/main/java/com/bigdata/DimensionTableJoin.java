package com.bigdata;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 实时数仓需要事实表关联维度表，维度表一般在外部存储
 */
public class DimensionTableJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        FlinkKafkaConsumer<String> fc = new FlinkKafkaConsumer<>("jaa,hotitem", new SimpleStringSchema(), prop);
        fc.setStartFromLatest();
        DataStreamSource<String> source = bsEnv.addSource(fc);
        //source注册成表
        //维表的流，注册成表
        RedisLookupableTableSource tableSource = RedisLookupableTableSource.Builder.newBuilder().withFieldNames(new String[]{"id", "name"})
                .withFieldTypes(new TypeInformation[]{Types.STRING, Types.STRING})
                .build();

//        tableEnv.registerTableSource("info", tableSource);

        //执行sql

        bsEnv.execute("redis");
    }
}
