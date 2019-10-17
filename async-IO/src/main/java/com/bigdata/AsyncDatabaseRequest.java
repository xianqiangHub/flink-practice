package com.bigdata;


import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

public class AsyncDatabaseRequest implements AsyncFunction<String, String> {
    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {

    }

    @Override
    public void timeout(String input, ResultFuture<String> resultFuture) throws Exception {

    }
}
