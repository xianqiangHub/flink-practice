package com.bigdata.table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MyTableAPI {
    /**
     * flink1.8有7个tablenvironment，
     * 到1.9有5个：TableEnvironment，StreamTableEnvironment（java,scala），BatchTableEnvironment （java,scala）
     * blink的planner有两个 TableEnvironment，StreamTableEnvironment
     * <p>
     * 1、TableEnvironment是顶级接口 已经支持了 StreamingMode 和 BatchMode 两种模式
     * 2、StreamTableEnvironment提供了 DataStream 和 Table 之间相互转换
     * 3、BatchTableEnvironment提供了 DataSet 和 Table 之间相互转换
     * <p>
     * blink vs flink
     * flink1.9默认是flink，更加稳定
     * Blink Planner 对代码生成机制做了改进、对部分算子进行了优化，提供了丰富实用的新功能，如维表 join、Top N、MiniBatch、流式去重、聚合场景的数据倾斜优化等新功能。
     * Blink Planner 的优化策略是基于公共子图的优化算法，包含了基于成本的优化（CBO）和基于规则的优化(CRO)两种策略，优化更为全面。同时，Blink Planner 支持从 catalog 中获取数据源的统计信息，这对CBO优化非常重要。
     * Blink Planner 提供了更多的内置函数，更标准的 SQL 支持，
     * Blink 不支持ExternalCatalog
     * <p>
     * Table API & SQL 未来
     * 1、Dynamic Tables
     * Dynamic Table 就是传统意义上的表，只不过表中的数据是会变化更新的。Flink 提出 Stream <–> Dynamic Table 之间是可以等价转换的。不过这需要引入Retraction机制。有机会的话，我会专门写一篇文章来介绍。
     * 2、Joins
     * 包括了支持流与流的 Join，以及流与表的 Join。
     * 3、SQL 客户端
     * 目前 SQL 是需要内嵌到 Java/Scala 代码中运行的，不是纯 SQL 的使用方式。未来需要支持 SQL 客户端执行提交 SQL 纯文本运行任务。
     * 4、并行度设置
     * 目前 Table API & SQL 是无法设置并行度的，这使得 Table API 看起来仍像个玩具。
     */

    public static void main(String[] args) throws Exception {

        // 场景一 flink old planner Streaming query
//        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
//        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
////       or  TableEnvironment fsTableEnv = TableEnvironment.create(fsSettings);
        //场景二  flink old planner batch query
//        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(fbEnv);
        //场景三 blink Streaming query
//        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
//////       or  TableEnvironment bsTableEnv = TableEnvironment.create(bsSettings);
        //场景三 blink batch query
//        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
//        TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);
        //******************************************************************************************************************************

        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        FlinkKafkaConsumer<String> fc = new FlinkKafkaConsumer<>("jaa,hotitem", new SimpleStringSchema(), prop);
//        new FlinkKafkaConsumer<>("jaa,hotitem", new KeyedDeserializationSchema(), prop);
        fc.setStartFromLatest();
        DataStreamSource<String> source = bsEnv.addSource(fc);
        //blink的查询计划
//        bsTableEnv.explain();
        SingleOutputStreamOperator<ClickLog> mapSource = source.map(new MapFunction<String, ClickLog>() {

            @Override
            public ClickLog map(String input) throws Exception {
                String[] split = input.split(",");

                return new ClickLog(split[0], Timestamp.valueOf(split[1]), split[2]);
            }
        });
        //split select print  可以切成两个流打印，独立操作不干扰
        DataStream<ClickLog> select1 = mapSource.split(new OutputSelector<ClickLog>() {
            @Override
            public Iterable<String> select(ClickLog value) {
                List<String> output = new ArrayList<String>();
                if (value.getUser().startsWith("j")) {
                    output.add("jia");
                } else {
                    output.add("jack");
                }
                return output;
            }
        }).select("jia");
        DataStream<ClickLog> select2 = mapSource.split(new OutputSelector<ClickLog>() {
            @Override
            public Iterable<String> select(ClickLog value) {
                List<String> output = new ArrayList<String>();
                if (value.getUser().startsWith("j")) {
                    output.add("jia");
                } else {
                    output.add("ack");
                }
                return output;
            }
        }).select("ack");

//        mapSource.print();

//        Table table = bsTableEnv.fromDataStream(mapSource, "kafka");//不能放数据源需要operator

//        Table kafka = table.select("kafka");
//        bsTableEnv.toRetractStream(kafka, Row.class).print();  Row ??
//bsTableEnv.toAppendStream(kafka);

        bsTableEnv.registerDataStream("click_table1", select1);
        bsTableEnv.registerDataStream("click_table2", select2);

        //Timestamp从MySQL数据库取出的字符串转换为LocalDateTime
//        Table table = bsTableEnv.sqlQuery("select user,count(url) as cnt from click_table group by user");
        Table table = bsTableEnv.sqlQuery("select * from click_table1 as a join click_table2 as b on a.url = b.url");

//        table.printSchema();
//               root
//                |-- cTime: TIMESTAMP(3)
//                |-- url: STRING
//                |-- user: STRING

//        bsTableEnv.toRetractStream(table, ClickLog.class).map(new MapFunction<Tuple2<Boolean, ClickLog>, String>() {
//            @Override
//            public String map(Tuple2<Boolean, ClickLog> input) throws Exception {
////                System.out.println(input.f0);
//
//                return input.f1.toString();
//            }
//        }).print();

        //流式sql的更新，不是说sql的更新操作，是SQL的计算，结果表的变化，会删除旧数据插入新数据，标识true和false
        //如果sink为可修改的数据库或者connect，实现更新
        //比如count 再来一条会3> (false,jack,3)     3> (true,jack,4)
        bsTableEnv.toRetractStream(table, Row.class).print();
//        System.out.println(table.toString());  UnnamedTable$0

        /**
         *  //表转化为流 toAppendStream   toRetractStream
         *追加模式：动态Table仅通过INSERT更改修改时才能使用此模式，如果更新或删除操作使用追加模式会失败报错
         *缩进模式：始终可以使用此模式。返回值是boolean类型。它用true或false来标记数据的插入和撤回，
         *    返回true代表数据插入，false代表数据的撤回可以适用于更新，删除等场景
         */

//        // SQL update with a registered table
//// create and register a TableSink
//        TableSink csvSink = new CsvTableSink("/path/to/file", ...);
//        String[] fieldNames = {"product", "amount"};
//        TypeInformation[] fieldTypes = {Types.STRING, Types.INT};
//        tableEnv.registerTableSink("RubberOrders", fieldNames, fieldTypes, csvSink);
//// run a SQL update query on the Table and emit the result to the TableSink
//        tableEnv.sqlUpdate(
//                "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
//
        bsEnv.execute("kafkaTable"); //没有流不会阻塞
    }
}
