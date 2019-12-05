package com.bigdata.udaf

import java.util.Properties

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.ConsumerConfig

case class DevData(devId: String, status: Int, times: Long)

/**
  * 设备状态上线/下线数量统计，上游采集设备状态发送到Kafka中，最开始是一个上线状态，此时统计到上线数量+1，
  * 过了一段时间该设备下线了，收到的下线的状态，那么此时应该是上线数量-1，下线数量+1
  * 当前实时的在线和不在线的设备量
  * 1、获取每个设备当前的状态
  * 2、按状态分组，统计
  * 3、Retract
  */
object Test {

  def main(args: Array[String]): Unit = {

    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env, bsSettings)
    tabEnv.registerFunction("latestTimeUdf", new MyUDAF())
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val kafkaConfig = new Properties()
    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test1")

    val consumer = new FlinkKafkaConsumer[String]("topic1", new SimpleStringSchema, kafkaConfig)
    val ds = env.addSource(consumer)
      .map(new MapFunction[String, DevData] {
        override def map(value: String) = {
          val a = value.split(",")
          DevData(a(0), a(1).toInt, a(2).toLong)
        }
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[DevData](Time.milliseconds(1000)) {
      override
      def extractTimestamp(element: DevData): Long
      = element.times
    })

    tabEnv.registerDataStream("tbl1", ds, "devId, status, times")
    val dw = tabEnv.sqlQuery(
      """
        select st,count(*) from (
                select latestTimeUdf(status,times) st,devId from tbl1 group by devId
                ) a group by st
      """.stripMargin)

    //        dw.writeToSink()
    //    tabEnv.toRetractStream(dw, classOf[Row]).print()

    env.execute()
  }

}
