package com.bigdata.JoinAndCogroup

import java.lang
import java.util.Properties

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util
import org.apache.kafka.clients.consumer.ConsumerConfig

case class Order(id: String, gdsId: String, amount: Double)

case class Gds(id: String, name: String)

case class RsInfo(orderId: String, gdsId: String, amount: Double, gdsName: String)

/**
  * 实现原理是：
  * 首先在两条流上加上标签 --》union合并两个流 --》根据key生成keyStream --》剩下是window的操作
  * 相同key的两条流合并在apply中，（流中的key来自不同的流，有标签的，后面再拆分成两个Iterable再到list）
  *  1. 在用户定义CoGroupFunction 被CoGroupWindowFunction包装之后，
  * 会接着被InternalIterableWindowFunction包装，一个窗口相同key的所有数据都会在一个Iterable中，
  * 会将其传给CoGroupWindowFunction
  *2. 在CoGroupWindowFunction中，会将不同流的数据区分开来得到两个list,传给用户自定义的CoGroupFunction中
  */
object SourceCogroup {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val kafkaConfig = new Properties()
    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test1")
    val orderConsumer = new FlinkKafkaConsumer010[String]("topic1", new SimpleStringSchema, kafkaConfig)
    val gdsConsumer = new FlinkKafkaConsumer010[String]("topic2", new SimpleStringSchema, kafkaConfig)
    val orderDs = env.addSource(orderConsumer)
      .map(x => {
        val a = x.split(",")
        Order(a(0), a(1), a(2).toDouble)
      })
    val gdsDs = env.addSource(gdsConsumer)
      .map(x => {
        val a = x.split(",")
        Gds(a(0), a(1))
      })
    //两条流key会进同一个机器，有Key的shuffle 就可能 数据倾斜
    orderDs.coGroup(gdsDs)
      .where(_.gdsId) // orderDs 中选择key
      .equalTo(_.id) //gdsDs中选择key
      //后面转化为一个窗口操作
      .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
      .apply(new CoGroupFunction[Order, Gds, RsInfo] {

        override def coGroup(first: lang.Iterable[Order], second: lang.Iterable[Gds], out: util.Collector[RsInfo]) = {
          //得到两个流中相同key的集合
        }
      })
    env.execute()
  }
}

/**
  * 自定义实现 left join
  */
//overridedef coGroup(first: lang.Iterable[Order], second: lang.Iterable[Gds],out:Collector[RsInfo]):Unit={
//
//  first.foreach(x =>{
//  if(!second.isEmpty){
//  second.foreach(y=>{
//  out.collect(newRsInfo(x.id,x.gdsId,x.amount,y.name))
//})
//}
//  if(second.isEmpty){
//  out.collect(newRsInfo(x.id,x.gdsId,x.amount,null))
//}
//})
//}