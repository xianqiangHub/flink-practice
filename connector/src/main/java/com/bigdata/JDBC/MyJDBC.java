package com.bigdata.JDBC;

/**
 * 一个是JDBC source 提高并行度。。
 * Flink在读取JDBC表时，为了加快速度，通常可以并发的方式读取，只需要增加以下几个参数：
 * 'connector.read.partition.column'='id',
 * 'connector.read.partition.lower-bound'='1',
 * 'connector.read.partition.upper-bound'='1000',
 * 'connector.read.partition.num'='10'
 * <p>
 * sink的话：
 * 一种 实现sinkfunction 里面自己写JDBC
 * 一种 使用使用jdbc写出  .writeUsingOutputFormat
 */
public class MyJDBC {

    public static void main(String[] args) {


    }
}
