package com.bigdata.example.uv;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class TestUVDay {
    public static void main(String[] args) throws Exception {

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        final String TOPIC_IN = "records";
        final String TOPIC_OUT = "recordOut";

        // kafka source
        tEnv.connect(
                new Kafka()
                        .version("universal")
                        .topic(TOPIC_IN)
                        .property("bootstrap.servers", "borker3:9092,borker1:9092,borker2:9092")
                        .property("group.id", "test")
                        .startFromLatest())
                .withFormat(new Json().deriveSchema())
                .withSchema(new Schema().field("record", Types.STRING))
                .inAppendMode()
                .registerTableSource("recordInfo");


        Table sqlQuery = tEnv.sqlQuery("select * from recordInfo");
        DataStream<Row> dataStream = tEnv.toAppendStream(sqlQuery, Row.class);
        dataStream.print();

//        tEnv.connect(
//                new Kafka()
//                        .version("universal")
//                        .topic(TOPIC_OUT)
//                        .property("bootstrap.servers", "borker3:9092,borker1:9092,borker2:9092")
//                        .sinkPartitionerFixed())
//                .withFormat(new Json().deriveSchema())
//                .withSchema(new Schema().field("record", Types.STRING))
//                .inAppendMode()
//                .registerTableSink("recordOut");
//
//        tEnv.sqlUpdate("insert into recordOut select record from recordInfo");

        System.out.println(env.getExecutionPlan());
//        env.execute("aa");
    }
}
