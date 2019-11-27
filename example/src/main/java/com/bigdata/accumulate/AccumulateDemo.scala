package com.bigdata.accumulate

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
  * Flink的累加器使用
  * 累加器用于并行执行的分布式累加
  * flink也是累加个task的和，当程序结束后才显示累加值
  * spark的累加器试driver端初始化，在excutor端累加，driver端获取
  */
object AccumulateDemo {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.fromElements("Hello Jason What are you doing Hello world")
    val counts = text
      .flatMap(_.toLowerCase.split(" "))
      .map(new RichMapFunction[String, String] {
        //创建累加器
        val acc = new IntCounter()

        override def open(parameters: Configuration): Unit = {
          super.open(parameters)
          //注册累加器
          getRuntimeContext.addAccumulator("accumulator", acc)
        }

        override def map(in: String): String = {
          //使用累加器
          this.acc.add(1)
          in
        }
      }).map((_, 1))
      .groupBy(0)
      .sum(1)

    //    counts.print()
    counts.writeAsText("d:/test.txt/").setParallelism(1)
    //在print后面不用加excutor,已经触发执行
    val res = env.execute("Accumulator Test")
    //获取累加器的结果
    val num = res.getAccumulatorResult[Int]("accumulator")
    println(num)
  }
}

