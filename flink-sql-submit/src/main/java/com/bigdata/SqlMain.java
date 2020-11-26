package com.bigdata;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;

/**
 * 执行flinksql的时候，还是看一下执行计划，看是否和预期的一样
 * 方式：
 * System.out.println(env.getExecutionPlan());
 * //        env.execute();
 * 通过网站https://flink.apache.org/visualizer/输入json
 * 比如读来的流经过计算之后再不同的计算分发到不同的sink，是否会重复计算(可以通过执行计划看)
 * 1、datastream两个sink用到中间的DS不会重复计算，觉得可以理解为数据分发到不同的计算方向（spark的是缓存RDD或
 * 直接checkpoint切断血缘关系，要理解的是spark和fink的处理数据模式，spark是优先在数据所在的节点计算，flink是在一个
 * 节点起一个任务，数据过来进行处理）
 * 2、flink的sql，比如中间计算的临时表，走不同的sink的话，从source都会重复计算，可以理解为flinksql的执行先生产物理计划
 * 两个物理计划都从头开始
 */
public class SqlMain {

    public static void main(String[] args) {

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
//       or  TableEnvironment fsTableEnv = TableEnvironment.create(fsSettings);
        fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        fsEnv.setParallelism(1);

        tEnv.connect(
                new Kafka()
                        .version("0.10")
                        //   "0.8", "0.9", "0.10", "0.11", and "universal"
                        .topic("jsontest")
                        .property("bootstrap.servers", "localhost:9093")
                        .property("group.id", "test")
                        .startFromLatest()
        )
                .withFormat(
                        new Json()
                                .failOnMissingField(false)
                                .deriveSchema()
                )
                .withSchema(

                        new Schema()
                                .field("rowtime", Types.SQL_TIMESTAMP)
                                .rowtime(new Rowtime()
                                        .timestampsFromField("eventtime")
                                        .watermarksPeriodicBounded(2000)
                                )
                                .field("fruit", Types.STRING)
                                .field("number", Types.INT)
                )
                .inAppendMode().createTemporaryTable("source");


        tEnv.connect(
                new Kafka()
                        .version("0.10")
                        //   "0.8", "0.9", "0.10", "0.11", and "universal"
                        .topic("test")
                        .property("acks", "all")
                        .property("retries", "0")
                        .property("batch.size", "16384")
                        .property("linger.ms", "10")
                        .property("bootstrap.servers", "localhost:9093")
                        .sinkPartitionerFixed()
        ).inAppendMode()
                .withFormat(
                        new Json().deriveSchema()
                )
                .withSchema(
                        new Schema()
                                .field("fruit", Types.STRING)
                                .field("total", Types.INT)
                                .field("time", Types.SQL_TIMESTAMP)
                )
                .createTemporaryTable("source");

        tEnv.connect(
                new Kafka()
                        .version("0.10")
                        //   "0.8", "0.9", "0.10", "0.11", and "universal"
                        .topic("test")
                        .property("acks", "all")
                        .property("retries", "0")
                        .property("batch.size", "16384")
                        .property("linger.ms", "10")
                        .property("bootstrap.servers", "localhost:9093")
                        .sinkPartitionerFixed()
        ).inAppendMode()
                .withFormat(
                        new Json().deriveSchema()
                )
                .withSchema(
                        new Schema()
                                .field("fruit", Types.STRING)
                                .field("total", Types.INT)
                                .field("time", Types.SQL_TIMESTAMP)
                )
                .createTemporaryTable("source");

        Table table = tEnv.sqlQuery("select * from source");
        tEnv.registerTable("view", table);


        tEnv.sqlUpdate("insert into sink select fruit,sum(number),TUMBLE_END(rowtime, INTERVAL '5' SECOND) from view group by fruit,TUMBLE(rowtime, INTERVAL '5' SECOND)");
        tEnv.sqlUpdate("insert into sink1 select fruit,sum(number),TUMBLE_END(rowtime, INTERVAL '5' SECOND) from view group by fruit,TUMBLE(rowtime, INTERVAL '5' SECOND)");

        System.out.println(fsEnv.getExecutionPlan());
//        env.execute();
    }
}

//
//    val create_sql =
//            """
//        | CREATE TABLE user_visit (
//        |    user_name VARCHAR,
//        |    ts timestamp
//        |) WITH (
//        |    'connector.type' = 'kafka',
//        |    'connector.version' = '0.10',
//        |    'connector.topic' = 'flink-test-05',
//        |    'connector.startup-mode' = 'latest-offset',
//        |    'connector.properties.0.key' = 'zookeeper.connect',
//        |    'connector.properties.0.value' = '192.168.17.24,192.168.17.25,192.168.17.26',
//        |    'connector.properties.1.key' = 'bootstrap.servers',
//        |    'connector.properties.1.value' = '192.168.17.26:9092,192.168.17.27:9092,192.168.17.28:9092',
//        |    'update-mode' = 'append',
//        |    'format.type' = 'json',
//        |    'format.derive-schema' = 'true'
//        |)
//        |""".stripMargin
//        println(create_sql)
//        val sink_sql =
//        """
//        |CREATE TABLE pvuv_sink (
//        |    dt VARCHAR,
//        |    pv BIGINT,
//        |    uv BIGINT
//        |) WITH (
//        |    'connector.type' = 'jdbc',
//        |    'connector.url' = 'jdbc:mysql://192.168.17.24:3306/flink_test',
//        |    'connector.table' = 'pvuv_sink',
//        |    'connector.username' = 'root',
//        |    'connector.password' = '123456',
//        |    'connector.write.flush.max-rows' = '1'
//        |)
//        |""".stripMargin
//        tEnv.sqlUpdate(create_sql)
//        tEnv.sqlUpdate(sink_sql)
//        tEnv.sqlUpdate(
//        """
//        |INSERT INTO pvuv_sink
//        |SELECT
//        | time_convert(ts) dt,
//        |  COUNT(*) AS pv,
//        |  COUNT(DISTINCT user_name) AS uv
//        |FROM user_visit
//        |GROUP BY time_convert(ts)
//        |
//        |""".stripMargin)
