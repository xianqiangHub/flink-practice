package com.bigdata.distinct

import java.util.Properties

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
  * case:广告在一小时被点击人数
  * 这个方式很奇怪，没有使用window实现，而是自己实现window加逻辑，也可以加深对window处理的理解，包括数据分配
  * 状态清理等
  *
  * https://mp.weixin.qq.com/s/yvrCnHqp3kR8pAEKWpRlAA
  */
object WindowDistinct {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val kafkaConfig = new Properties()
    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test1")
    val consumer = new FlinkKafkaConsumer[String]("topic1", new SimpleStringSchema, kafkaConfig)

    val ds = env.addSource(consumer)
    ds.map(new MapFunction[String, AdData] {
      override def map(value: String) = {
        val s = value.split(",")
        AdData(s(0).toInt, s(1), s(2).toLong)
      }
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[AdData](Time.minutes(1)) {
      override def extractTimestamp(element: AdData): Long = element.time
    })
      .keyBy(new KeySelector[AdData, AdKey] {
        //分组的key不要局限于字段
        override def getKey(value: AdData) = {
          //拿到窗口的结束时间
          val endTime = TimeWindow.getWindowStartWithOffset(value.time, 0,
            Time.hours(1).toMilliseconds) + Time.hours(1).toMilliseconds
          //根据广告和广告所在的窗口（窗口的endtime）分流
          AdKey(value.id, endTime)
        }
      }).process(new DistinctProcessFunction()).uid("process")

    env.execute(WindowDistinct.getClass.getSimpleName)
  }
}

/**
  * 中间使用valuestate来计数，因为mapstate获取总数要遍历，放在valuestate直接获取
  */
class DistinctProcessFunction extends KeyedProcessFunction[AdKey, AdData, Void] {
  var devIdState: MapState[String, Int] = _
  var devIdStateDesc: MapStateDescriptor[String, Int] = _

  var countState: ValueState[Long] = _
  var countStateDesc: ValueStateDescriptor[Long] = _

  override def open(parameters: Configuration): Unit = {

    devIdStateDesc = new MapStateDescriptor[String, Int]("devIdState", TypeInformation.of(classOf[String]), TypeInformation.of(classOf[Int]))
    devIdState = getRuntimeContext.getMapState(devIdStateDesc)

    countStateDesc = new ValueStateDescriptor[Long]("countState", TypeInformation.of(classOf[Long]))
    countState = getRuntimeContext.getState(countStateDesc)
  }

  override def processElement(value: AdData, ctx: KeyedProcessFunction[AdKey, AdData, Void]#Context, out: Collector[Void]): Unit = {

    //模拟窗口的延时数据处理，窗口可以用侧输出
    val currW = ctx.timerService().currentWatermark()
    if (ctx.getCurrentKey.time + 1 <= currW) {
      println("late data:" + value)
      return
    }

    val devId = value.devId
    devIdState.get(devId) match {
      case 1 => {
        //表示已经存在
      }
      case _ => {
        //表示不存在
        devIdState.put(devId, 1)
        val c = countState.value()
        countState.update(c + 1)
        //还需要注册一个定时器   注册了定时器自己实现定时器
        //模拟窗口触发了进行状态清理
        ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey.time + 1)
      }
    }
    println(countState.value()) //输出
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[AdKey, AdData, Void]#OnTimerContext, out: Collector[Void]): Unit = {
    println(timestamp + " exec clean~~~")
    println(countState.value())
    devIdState.clear()
    countState.clear()
  }
}

//广告数据
case class AdData(id: Int, devId: String, time: Long)

//分组数据
case class AdKey(id: Int, time: Long)
