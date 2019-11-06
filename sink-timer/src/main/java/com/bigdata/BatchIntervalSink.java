package com.bigdata;

import org.apache.kafka.common.utils.Java;

import java.util.List;

public class BatchIntervalSink extends CommonSinkOperate<java.lang.String> {

    //子类没写属性，直接用父类的属性
    public BatchIntervalSink(int batchSize, Long batchInterval) {
        super(batchSize, batchInterval);
    }


    @Override
    public void saveRecords(List<String> datas) {

//    }datas
    }
}
