package com.bigdata.io;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class AsyncReadRedis extends RichAsyncFunction<String, String> {
    //获取连接池的配置对象
    private JedisPoolConfig config = null;
    //获取连接池
    JedisPool jedisPool = null;
    //获取核心对象
    Jedis jedis = null;
    //Redis服务器IP
    private static String ADDR = "127.0.0.1";
    //Redis的端口号
    private static int PORT = 6379;
    //访问密码
    private static String AUTH = "XXXXXX";
    //等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
    private static int TIMEOUT = 10000;
    private static final Logger logger = LoggerFactory.getLogger(AsyncReadRedis.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        config = new JedisPoolConfig();
        jedisPool = new JedisPool(config, ADDR, PORT, TIMEOUT, AUTH, 5);
        jedis = jedisPool.getResource();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
        // 发起一个异步请求，返回结果的 future
        // CompletableFuture,当异步访问完成时，需要调用其方法进行处理.
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                String[] split = input.split(",");
                String name = split[1];
                String s = jedis.hget("AsyncReadRedis", name);
                return s;
            }
        }).thenAccept((String dbResult) -> {
            // 设置请求完成时的回调: 将结果传递给 collector
            resultFuture.complete(Collections.singleton(dbResult));
        });


    }

    @Override
    public void timeout(String input, ResultFuture<String> resultFuture) throws Exception {

    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
