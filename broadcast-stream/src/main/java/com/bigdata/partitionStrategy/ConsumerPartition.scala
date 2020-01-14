package com.bigdata.partitionStrategy

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
  * 并行度和数据重新分布
  * 比如并行度为 2 到并行度为 4 ，数据怎么分配，默认是自动分配
  * 但有个缺陷，比如默写节点数据过多，造成倾斜，所以可以更改分配策略，自带有以下策略，参考
  * https://cloud.tencent.com/developer/article/1561734
  * 下面是自定义分区规则，根据数据规则判断到哪个实例
  */
object ConsumerPartition {

  def main(args: Array[String]): Unit = {
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 获取当前执行环境的默认并行度
    val defaultParalleism = senv.getParallelism
    // 设置所有算子的并行度为4，表示所有算子的并行执行的实例数为4
    senv.setParallelism(4)
    val dataStream: DataStream[(Int, String)] = senv.fromElements((1, "123"), (2, "abc"), (3, "256"), (4, "zyx")
      , (5, "bcd"), (6, "666"))
    // 对(Int, String)中的第二个字段使用 MyPartitioner 中的重分布逻辑
    val partitioned = dataStream.partitionCustom(new MyPartitioner, 1)

    partitioned.print()
    senv.execute("partition custom transformation")
  }


  class MyPartitioner extends Partitioner[String] {
    val rand = scala.util.Random

    /**
      * key 泛型T 即根据哪个字段进行数据重分配，本例中是(Int, String)中的String
      * numPartitons 为当前有多少个并行实例
      * 函数返回值是一个Int 为该元素将被发送给下游第几个实例
      **/
    override def partition(key: String, numPartitions: Int): Int = {
      var randomNum = rand.nextInt(numPartitions / 2)
      // 如果字符串中包含数字，该元素将被路由到前半部分，否则将被路由到后半部分。
      if (key.exists(_.isDigit)) {
        return randomNum
      } else {
        return randomNum + numPartitions / 2
      }
    }
  }

}
