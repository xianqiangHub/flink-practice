package com.bigdata.udtf

import java.lang
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.kafka.clients.consumer.ConsumerConfig

case class RawData(devId: String, time: Long, data: String)

object Test {

  def main(args: Array[String]): Unit = {

    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env, bsSettings)
    //    val tabEnv = TableEnvironment.create(bsSettings)
    tabEnv.registerFunction("udtf", new MyUDTF) //注册
    val kafkaConfig = new Properties()
    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test1")
    //    {"devid":"dev01","time":1574944573000,"data":[{"type":"temperature","value":"10"},{"type":"battery","value":"1"}]}
    val consumer = new FlinkKafkaConsumer[String]("topic1", new SimpleStringSchema(), kafkaConfig)
    val ds = env.addSource(consumer).map[(String, lang.Long, String)](
      new MapFunction[String, (String, lang.Long, String)] {
        override def map(value: String) = {
          val obj = JSON.parseObject(value, classOf[RawData])
          Tuple3.apply(obj.devId, obj.time, obj.data)
        }
      })

    tabEnv.registerDataStream("tbl1", ds, "devId, time, data")
    //UDTF
//    在Flink SQL中使用TableFunction需要搭配LATERAL TABLE一起使用，将其认为是一张虚拟的表
//    整个过程就是一个Join with Table Function过程，左表(tbl1) 会join 右表(t1) 的每一条记录。
//    但是也存在另外一种情况右表(t1)没有输出但是也需要左表输出那么可以使用LEFT JOIN LATERAL TABLE，用法如下：
//    SELECT users, tag
//    FROM Orders LEFT JOIN LATERAL TABLE(unnest_udtf(tags)) t AS tag ON TRUE
//      对于右表没有输出会自动补上null。
    val rsTab = tabEnv.sqlQuery("select devId,`time`,`type`,`value` " +
      "from tbl1 , LATERAL TABLE(udtf(data)) as t (`type`,`value`) ")

    //      .writeToSink(new RetractStreamTableSink)

    //打印sql的执行计划 可以得到其抽象语法树、逻辑执行计划、物理执行计划
    //    println(tabEnv.explain(rsTab))

    env.execute()
  }

}
