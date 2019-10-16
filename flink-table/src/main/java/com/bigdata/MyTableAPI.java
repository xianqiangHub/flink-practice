package com.bigdata;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

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

    public static void main(String[] args) {

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
        FlinkKafkaConsumer<String> fc = new FlinkKafkaConsumer<>("hotitem", new SimpleStringSchema(), prop);
        fc.setStartFromLatest();
        DataStreamSource<String> source = bsEnv.addSource(fc);

        bsTableEnv.fromDataStream(source);



    }
}
