package com.bigdata.example.uv;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;


/**
 * .inRetractMode()   ///???????会报错 kafka不是各可撤回的流
 */
public class UVDay {
    public static void main(String[] args) throws Exception {

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

//       TableEnvironment fsTableEnv = TableEnvironment.create(fsSettings);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置表的参数
//        fsTableEnv.getConfig().getConfiguration().setString("", "");

        tEnv.registerFunction("DateUtil", new DateUtil()); //注册自定义函数
        tEnv.connect(
                new Kafka()
                        .version("universal")
                        //   "0.8", "0.9", "0.10", "0.11", and "universal"
                        .topic("jsontest")
                        .property("bootstrap.servers", "borker3:9092,borker1:9092,borker2:9092")
                        .property("group.id", "test")
                        .startFromLatest()
        )
                .withFormat(
                        new Json()
                                .failOnMissingField(false)
                                .deriveSchema() //从我们指定的schema里逆推
                )
                .withSchema(

                        new Schema()
//                                .proctime()      // optional: declares this field as a processing-time attribute
// Converts an existing LONG or SQL_TIMESTAMP field in the input into the rowtime attribute..rowtime(  new Rowtime()    .timestampsFromField("ts_field")    // required: original field name in the input)
// Converts the assigned timestamps from a DataStream API record into the rowtime attribute// and thus preserves the assigned timestamps from the source.// This requires a source that assigns timestamps (e.g., Kafka 0.10+)..rowtime(  new Rowtime()    .timestampsFromSource())
// Sets a custom timestamp extractor to be used for the rowtime attribute.// The extractor must extend `org.apache.flink.table.sources.tsextractors.TimestampExtractor`..rowtime(  new Rowtime()    .timestampsFromExtractor(...))

                                // Sets a watermark strategy for ascending rowtime attributes. Emits a watermark of the maximum// observed timestamp so far minus 1. Rows that have a timestamp equal to the max timestamp// are not late..rowtime(  new Rowtime()    .watermarksPeriodicAscending())
// Sets a built-in watermark strategy for rowtime attributes which are out-of-order by a bounded time interval.// Emits watermarks which are the maximum observed timestamp minus the specified delay..rowtime(  new Rowtime()    .watermarksPeriodicBounded(2000)    // delay in milliseconds)
// Sets a built-in watermark strategy which indicates the watermarks should be preserved from the// underlying DataStream API and thus preserves the assigned watermarks from the source..rowtime(  new Rowtime()    .watermarksFromSource())
                                .field("rowtime", Types.SQL_TIMESTAMP)
                                .rowtime(new Rowtime()
                                        .timestampsFromField("eventtime") //
                                        .watermarksPeriodicBounded(2000)  //
                                )  //optional: declares this field as a event-time attribute
                                .field("fruit", Types.STRING)
                                .field("number", Types.INT)
//                                .from()//optional: original field in the input that is referenced/aliased by this field
                )
                .inAppendMode()
//                .inRetractMode()   ///???????会报错 kafka不是各可撤回的流
                .registerTableSource("recordInfo"); //表名

        //设置状态的保存时间
//        TableConfig config = fsTableEnv.getConfig();
//        config.setIdleStateRetentionTime(Time.minutes(10), Time.minutes(15));

        // 計算天級別的uv
//        Table table = tEnv.sqlQuery("select  DateUtil(rowtime),count(distinct fruit) from source group by DateUtil(rowtime)");

        // 计算小时级别uv
//        Table table = fsTableEnv.sqlQuery("select  DateUtil(rowtime,'yyyyMMddHH'),count(distinct fruit) from source group by DateUtil(rowtime,'yyyyMMddHH')");
        Table table = tEnv.sqlQuery("select * from recordInfo");

        /**
         *  有三个问题，第一，优化，因为按天分组 聚合 数据量较大 count(distinct )
         *
         *  第二，状态保存，在动态表上做连续查询，比如uv只需要当天的数据，可以清理掉一天之前的数据
         *
         *  第三，如果延迟到来的数据的状态被清理掉了，这条数据就单独成一条了
         */

//        table.printSchema();

        tEnv.toRetractStream(table, Row.class).addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
            @Override
            public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                System.out.println(value.f1.toString());
            }
        });


//        DataStream<Row> dataStream = fsTableEnv.toAppendStream(table, Row.class);
//        table.printSchema();
//        dataStream.print();

        System.out.println(env.getExecutionPlan());
//        fsEnv.execute("UVDay");
    }
}

