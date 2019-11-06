package com.bigdata;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 此类打成jar包放到flink的lib目录下，flink运行加载lib目录下的jar包
 */
public class KafkaAppender extends AppenderSkeleton {

    private static final ExecutorService exector = Executors.newFixedThreadPool(2);

    private String logFile; //这个变量是taskManager启动传入的参数，表示的是日志路径，路径里面包含了applicationId
    private String kafkaBroker;
    private String topic;
    private String ispre;
    private String logEvenl; //ERROR INFO


    @Override
    protected void append(LoggingEvent event) {

        //try catch 日志写入kafka的成功与否不应该影响flink任务的执行
        try {
            Level level = event.getLevel();
            if (logEvenl.contains(level.toString())) {
                //异步执行发送日志
//                exector.execute(new KafkaTask(ecent,this));  //json格式
            }
        } catch (Exception e) {
            e.printStackTrace();
        };
    }

    public String getLogFile() {
        return logFile;
    }

    public void setLogFile(String logFile) {
        this.logFile = logFile;
    }

    public String getKafkaBroker() {
        return kafkaBroker;
    }

    public void setKafkaBroker(String kafkaBroker) {
        this.kafkaBroker = kafkaBroker;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getIspre() {
        return ispre;
    }

    public void setIspre(String ispre) {
        this.ispre = ispre;
    }

    public String getLogEvenl() {
        return logEvenl;
    }

    public void setLogEvenl(String logEvenl) {
        this.logEvenl = logEvenl;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean requiresLayout() {
        return false;
    }
}
