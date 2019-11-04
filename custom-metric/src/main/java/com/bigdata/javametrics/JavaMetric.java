package com.bigdata.javametrics;

import com.codahale.metrics.MetricRegistry;

/**
 * Metrics 数据展示
 * Metircs 提供了 Report 接口，用于展示 metrics 获取到的统计数据。
 * metrics-core中主要实现了四种 reporter： JMX, console, SLF4J, 和 CSV。
 * 在本文的例子中，我们使用 ConsoleReporter 。
 */

//监控  统计
public class JavaMetric {

    public static void main(String[] args) {

        //MetricRegistry类是Metrics的核心，它是存放应用中所有metrics的容器。也是我们使用 Metrics 库的起点。
        MetricRegistry registry = new MetricRegistry();

        MetricRegistry.name("myself", "one");
        MetricRegistry.name("myself", "two");

        //http://wuchong.me/blog/2015/08/01/getting-started-with-metrics/


    }
}
