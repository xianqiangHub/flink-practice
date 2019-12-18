package com.bigdata.example;

/**
 * flinksql使用的优化
 * https://help.aliyun.com/document_detail/98981.html?spm=a2c4g.11186623.6.614.86286d163E6NmE
 * 1.MiniBatch优化
 * Configuration configuration = tEnv.getConfig().getConfiguration();
 * // set low-level key-value options
 * configuration.setString("table.exec.mini-batch.enabled", "true"); // enable mini-batch optimization
 * configuration.setString("table.exec.mini-batch.allow-latency", "5 s"); // use 5 seconds to buffer input records
 * configuration.setString("table.exec.mini-batch.size", "5000"); // the maximum number of records can be buffered by each aggregate operator task
 * <p>
 * 2.分组聚合
 * configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE"); // enable two-phase, i.e. local-global aggregation
 * <p>
 * 3.COUNT(DISTINCT a)
 * tEnv.getConfig()        // access high-level configuration
 * .getConfiguration()   // set low-level key-value options
 * .setString("table.optimizer.distinct-agg.split.enabled", "true");  // enable distinct agg split
 */
public class Test {

    public static void main(String[] args) {

    }
}
