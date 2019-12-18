package com.bigdata.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * 输入一条记录，返回一条记录
 */
public class MyUDF extends ScalarFunction {

    //需要自己定义 名字为 eval方法
    // 在ProcessOperator中，其function即为自动生成的函数，在其processElement方法中，会调用我们自定义的eval方法，完成相关的转化逻辑。
    public static String eval(long timestamp) {

        return "";
    }
}
