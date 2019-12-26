package com.bigdata.sinkMultithreading;

import com.google.common.collect.Queues;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 当sink端写入外部数据库的时候，防止每条数据都访问一次，给数据库压力，可以积攒部分数据写出，如果同步连接的话，
 * 等待响应的时间CPU处于空闲状态，CPU利用率低并且吞吐量小，可以使用异步，中间只有积攒数据的时间
 *
 * 当有些数据库客户端没有异步客户端的话，可以使用多线程模拟，下面就是flink的sink多线程保证exactly once
 *
 * https://mp.weixin.qq.com/s/YWKw8jhTdaDoppkcoYYf7g
 */
public class Multithreading {

    public static void main(String[] args) {


    }

    public class MultiThreadConsumerSink extends RichSinkFunction<String> {
        // Client 线程的默认数量
        private final int DEFAULT_CLIENT_THREAD_NUM = 5;
        // 数据缓冲队列的默认容量
        private final int DEFAULT_QUEUE_CAPACITY = 5000;

        private LinkedBlockingQueue<String> bufferQueue;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // new 一个容量为 DEFAULT_CLIENT_THREAD_NUM 的线程池
            ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(DEFAULT_CLIENT_THREAD_NUM, DEFAULT_CLIENT_THREAD_NUM,
                    0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            // new 一个容量为 DEFAULT_QUEUE_CAPACITY 的数据缓冲队列
            this.bufferQueue = Queues.newLinkedBlockingQueue(DEFAULT_QUEUE_CAPACITY);
            // 创建并开启消费者线程
            MultiThreadConsumerClient consumerClient = new MultiThreadConsumerClient(bufferQueue);
            for (int i=0; i < DEFAULT_CLIENT_THREAD_NUM; i++) {
                threadPoolExecutor.execute(consumerClient);  //执行多线程=
            }
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            // 往 bufferQueue 的队尾添加数据
            bufferQueue.put(value);
        }
    }

    public class MultiThreadConsumerClient implements Runnable {

        private LinkedBlockingQueue<String> bufferQueue;

        public MultiThreadConsumerClient(LinkedBlockingQueue<String> bufferQueue) {
            this.bufferQueue = bufferQueue;
        }

        @Override
        public void run() {
            String entity;
            while (true){
                // 从 bufferQueue 的队首消费数据
                entity = bufferQueue.poll();
                // 执行 client 消费数据的逻辑
                doSomething(entity);
            }
        }
        // client 消费数据的逻辑
        private void doSomething(String entity) {
            // client 积攒批次并调用第三方 api
        }
    }
}
