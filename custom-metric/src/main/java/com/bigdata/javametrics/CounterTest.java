package com.bigdata.javametrics;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

import java.util.Queue;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Counter 就是计数器，Counter 只是用 Gauge 封装了 AtomicLong 。我们可以使用如下的方法，使得获得队列大小更加高效。
 */
public class CounterTest {

    public static Queue<String> q = new LinkedBlockingQueue<String>();

    public static Counter pendingJobs;

    public static Random random = new Random();

    public static void addJob(String job) {
//        Increment the counter by one.
        pendingJobs.inc();
        // Inserts the specified element into this queue
        q.offer(job);
    }

    public static String takeJob() {
        pendingJobs.dec();
        return q.poll();
    }

    public static void main(String[] args) throws InterruptedException {

        MetricRegistry registry = new MetricRegistry();
        ConsoleReporter reporter = ConsoleReporter.forRegistry(registry).build();
        //循环导出指标周期和单位
        reporter.start(1, TimeUnit.SECONDS);

        pendingJobs = registry.counter(MetricRegistry.name(Queue.class, "pending-jobs", "size"));

        int num = 1;
        while (true) {
            Thread.sleep(200);
            if (random.nextDouble() > 0.7) {
                pendingJobs.dec();
//                String job = takeJob();
//                System.out.println("take job : " + job);
            } else {
                pendingJobs.inc();
//                String job = "Job-" + num;
//                addJob(job);
//                System.out.println("add job : " + job);
            }
            num++;
        }
    }
}
