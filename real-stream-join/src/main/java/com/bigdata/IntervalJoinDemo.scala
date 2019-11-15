package com.bigdata

import java.util.Properties

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig

case class Order(orderId: String, userId: String, gdsId: String, amount: Double, addrId: String, time: Long)

case class Address(addrId: String, userId: String, address: String, time: Long)

case class RsInfo(orderId: String, userId: String, gdsId: String, amount: Double, addrId: String, address: String)

/**
  * 用windowjoin的时候，如果有一条流延时，可能不会落在同一个窗口join
  * intervaljoin的意思是一条流只需要另一条流的时间范围，只要确定延迟到  lower ---up
  * 1.必须是两条keyedstream
  * 2.必须是event time ，逻辑基于event time
  *
  * 实现逻辑核心在IntervalJoinOperator 类的process方法
  * 两条流connect之后keyby生成新的connectStream，两条流有自己的状态和对方的状态，左边的流通过双层for循环，去判断右边
  * 的流有没有在时间范围内的数据，右边的流也是双层for循环，遍历左边的流有没有自己的数据，有的话输出
  *
  * watermark用来触发清理过期的状态，各自注册timer
  *
  *
  */
object IntervalJoinDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(5000L) //覆盖，5s一个watermark
    env.setParallelism(1)

    val kafkaConfig = new Properties()
    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test1")

    val orderConsumer = new FlinkKafkaConsumer010[String]("topic1", new SimpleStringSchema, kafkaConfig)
    val addressConsumer = new FlinkKafkaConsumer010[String]("topic2", new SimpleStringSchema, kafkaConfig)

    val orderStream = env.addSource(orderConsumer)
      .map(x => {
        val a = x.split(",")
        Order(a(0), a(1), a(2), a(3).toDouble, a(4), a(5).toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Order](Time.seconds(10)) {
      override
      def extractTimestamp(element: Order): Long = element.time
    })
      .keyBy(_.addrId)

    val addressStream = env.addSource(addressConsumer)
      .map(x => {
        val a = x.split(",")
        Address(a(0), a(1), a(2), a(3).toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Address](Time.seconds(10)) {
      override
      def extractTimestamp(element: Address): Long
      = element.time
    })
      .keyBy(_.addrId)
    //***********************************************************************
    //是addressStream数据会迟到 1s - 5s
    orderStream.intervalJoin(addressStream)
      //leftElement.timestamp + lowerBound <= rightElement.timestamp <= leftElement.timestamp + upperBound
      .between(Time.seconds(1), Time.seconds(5))
      //得到相同key的两条数据
      .process(new ProcessJoinFunction[Order, Address, RsInfo] {
      override
      def processElement(left: Order, right: Address, ctx: ProcessJoinFunction[Order, Address, RsInfo]#Context, out: Collector[RsInfo]): Unit = {
        println("==在这里得到相同key的两条数据===")
        println("left:" + left)
        println("right:" + right)
      }
    })
    env.execute()
  }
}
