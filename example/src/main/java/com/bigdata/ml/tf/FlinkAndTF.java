package com.bigdata.ml.tf;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.Graph;
import org.tensorflow.Session;
import org.tensorflow.Tensor;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * 这个思路是使用再taskmanager，使用tensorflow的java API 调用模型
 *
 * https://my.oschina.net/112612/blog/3069578
 *
 * 以及天池的垃圾图片分类比赛的  AnalyticsZoo
 */

public class FlinkAndTF {
    private static final Logger logger = LoggerFactory.getLogger(FlinkAndTF.class);

    public static void main(String[] args) throws Exception {
//....
//        env.execute("");
    }

    static class DTCZabbixMapFunction implements MapFunction<String, Float> {

        @Override
        public Float map(String event) throws IOException {
            Graph graph = new Graph();
            byte[] graphBytes;
            Session session = null;
            FileInputStream fileInputStream = null;
            float message = 0f;
            try {
                fileInputStream = new FileInputStream("/your path/dtc-flink-zabbix/src/main/resources/model.pb");
                graphBytes = IOUtils.toByteArray(fileInputStream);
                graph.importGraphDef(graphBytes);
                session = new Session(graph);
                message = session.runner()
                        .feed("x", Tensor.create(Float.valueOf(event)))
                        .fetch("z").run().get(0).floatValue();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                session.close();
                fileInputStream.close();
                graph.close();
            }
            return message;
        }
    }
}


