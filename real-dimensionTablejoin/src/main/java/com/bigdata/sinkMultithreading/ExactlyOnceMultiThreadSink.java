package com.bigdata.sinkMultithreading;

import com.google.common.collect.Queues;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 在sink端使用多线程代替异步客户端，使用了队列防止某些写出慢的阻塞，这也导致了在任务失败时，队列中的数据会丢失
 * 现在做法是类似于flinkkafkaconsumer自己实现checkpointfunction，在snapshat时把消费的数据都刷写出去
 * 保证快照到的数据全部被刷写出去
 */
public class ExactlyOnceMultiThreadSink {

    public static void main(String[] args) {

    }

    public class MultiThreadConsumerSink extends RichSinkFunction<String> implements CheckpointedFunction {

        private Logger LOG = LoggerFactory.getLogger(MultiThreadConsumerSink.class);

        // Client 线程的默认数量
        private final int DEFAULT_CLIENT_THREAD_NUM = 5;
        // 数据缓冲队列的默认容量
        private final int DEFAULT_QUEUE_CAPACITY = 5000;

        private LinkedBlockingQueue<String> bufferQueue;
        private CyclicBarrier clientBarrier;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // new 一个容量为 DEFAULT_CLIENT_THREAD_NUM 的线程池
            ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(DEFAULT_CLIENT_THREAD_NUM, DEFAULT_CLIENT_THREAD_NUM,
                    0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            // new 一个容量为 DEFAULT_QUEUE_CAPACITY 的数据缓冲队列
            this.bufferQueue = Queues.newLinkedBlockingQueue(DEFAULT_QUEUE_CAPACITY);
            // barrier 需要拦截 (DEFAULT_CLIENT_THREAD_NUM + 1) 个线程
            this.clientBarrier = new CyclicBarrier(DEFAULT_CLIENT_THREAD_NUM + 1);
            // 创建并开启消费者线程
            MultiThreadConsumerClient consumerClient = new MultiThreadConsumerClient(bufferQueue, clientBarrier);
            for (int i = 0; i < DEFAULT_CLIENT_THREAD_NUM; i++) {
                threadPoolExecutor.execute(consumerClient);
            }
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            // 往 bufferQueue 的队尾添加数据
            bufferQueue.put(value);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            LOG.info("snapshotState : 所有的 client 准备 flush !!!");
            // barrier 开始等待
            clientBarrier.await();
        }

        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        }

    }

//    public class MultiThreadConsumerSink extends RichSinkFunction<String> {
//        // Client 线程的默认数量
//        private final int DEFAULT_CLIENT_THREAD_NUM = 5;
//        // 数据缓冲队列的默认容量
//        private final int DEFAULT_QUEUE_CAPACITY = 5000;
//
//        private LinkedBlockingQueue<String> bufferQueue;
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            super.open(parameters);
//            // new 一个容量为 DEFAULT_CLIENT_THREAD_NUM 的线程池
//            ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(DEFAULT_CLIENT_THREAD_NUM, DEFAULT_CLIENT_THREAD_NUM,
//                    0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
//            // new 一个容量为 DEFAULT_QUEUE_CAPACITY 的数据缓冲队列
//            this.bufferQueue = Queues.newLinkedBlockingQueue(DEFAULT_QUEUE_CAPACITY);
//            // 创建并开启消费者线程
//            MultiThreadConsumerClient consumerClient = new MultiThreadConsumerClient(bufferQueue);
//            for (int i = 0; i < DEFAULT_CLIENT_THREAD_NUM; i++) {
//                threadPoolExecutor.execute(consumerClient);
//            }
//        }
//
//        @Override
//        public void invoke(String value, Context context) throws Exception {
//            // 往 bufferQueue 的队尾添加数据
//            bufferQueue.put(value);
//        }
//    }

    public class MultiThreadConsumerClient implements Runnable {

        private LinkedBlockingQueue<String> bufferQueue;
        private CyclicBarrier clientBarrier;


        public MultiThreadConsumerClient(LinkedBlockingQueue<String> bufferQueue, CyclicBarrier clientBarrier) {
            this.bufferQueue = bufferQueue;
        }

        @Override
        public void run() {
            String entity;
            while (true) {
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
