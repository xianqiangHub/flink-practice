package com.bigdata.javametrics;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

/**
 * 最简单的度量指标，只有一个简单的返回值，例如，我们想衡量一个待处理队列中任务的个数
 */
public class GaugeTest {

    public static Queue<String> q = new LinkedList<String>();

    public static void main(String[] args) throws InterruptedException {
        MetricRegistry registry = new MetricRegistry();
        ConsoleReporter reporter = ConsoleReporter.forRegistry(registry).build();
        reporter.start(1, TimeUnit.SECONDS);

        registry.register(MetricRegistry.name(GaugeTest.class, "queue", "size"), new Gauge<Integer>() {

            public Integer getValue() {
                return q.size();
            }
        });

        while (true) {
            Thread.sleep(1000);
            q.add("Job-xxx");
        }
    }
}
