package com.bigdata.hbase;

/**
 * 如果维表的数据比较大，无法一次性全部加载到内存中，而在业务上也允许一定数据的延时，
 * 那么就可以使用LRU策略加载维表数据
 * <p>
 * 实现思路：
 * <p>
 * 1.使用Flink 异步IO RichAsyncFunction去异步读取hbase的数据，那么需要hbase 客户端支持异步读取，
 * 默认hbase客户端是同步，可使用hbase 提供的asynchbase 客户端；
 * 2.初始化一个Cache 并且设置最大缓存容量与数据过期时间；
 * 3.数据读取逻辑：先根据Key从Cache中查询value，如果能够查询到则返回，如果没有查询到结果则使用asynchbase查询数据，
 * 并且将查询的结果插入Cache中，然后返回
 */
public class HbaseMain {

    public static void main(String[] args) {


    }
}
