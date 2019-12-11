package com.bigdata.fileSource;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ReadLineSource extends RichParallelSourceFunction<String> {

    private String filePath;
    private boolean canceled = false;

    public ReadLineSource(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

        InputStream in = ReadLineSource.class.getClassLoader().getResourceAsStream(filePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
//        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        while (!canceled && reader.ready()) {
            String line = reader.readLine();
//            System.out.println("读取：".concat(line));
            sourceContext.collect(line);
            //100 ms
            Thread.sleep(1000);
        }
        sourceContext.close();
    }

    @Override
    public void cancel() {
        canceled = true;
    }

}
