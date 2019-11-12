package com.bigdata.hbase;

import akka.remote.EndpointWriter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hbase.async.HBaseClient;

import java.util.concurrent.TimeUnit;

public class HbaseAsyncLRU extends RichAsyncFunction<String, String> {

    private String zk;
    private String tableName;
    private Long maxSize;
    private Long ttl;

    public HbaseAsyncLRU() {
    }

    public HbaseAsyncLRU(String zk, String tableName, Long maxSize, Long ttl) {
        this.zk = zk;
        this.tableName = tableName;
        this.maxSize = maxSize;
        this.ttl = ttl;
    }

    private HBaseClient hBaseClient;
    private Cache<String, String> cache;

    @Override
    public void open(Configuration parameters) throws Exception {
        //hbase的异步客户端
        hBaseClient = new HBaseClient(zk);
        //LRU
        cache = CacheBuilder.newBuilder().maximumSize(maxSize)
                .expireAfterWrite(ttl, TimeUnit.SECONDS)
                .build();
    }

    //input element coming from an upstream task
    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
        //拿到redis的key
        String rowKey = "";
        String ifPresent = cache.getIfPresent(rowKey);

//        if (value != null) {
//            val newV: String = fillData(input, value)
//            resultFuture.complete(Collections.singleton(newV))
//            return
//        }
//        val get = new GetRequest(tableName, key)
//        hbaseClient.get(get).addCallbacks(new Callback[String, util.ArrayList[KeyValue]] {
//            override def call(t: util.ArrayList[KeyValue]): String = {
//                    val v = parseRs(t)
//                    cache.put(key, v)
//                    resultFuture.complete(Collections.singleton(v))
//                    ""
//            }
//        }, new Callback[String, Exception] {
//            override def call(t: Exception): String = {
//                    t.printStackTrace()
//                    resultFuture.complete(null)
//                    ""
//            }

    }

}
