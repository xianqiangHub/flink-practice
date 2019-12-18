package com.bigdata.example.uv;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class DateUtil extends ScalarFunction {
    public static String eval(long timestamp) {
        String result = "null";
        try {
            DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            result = sdf.format(new Timestamp(timestamp));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String eval(long ts, String format) {

        String result = "null";
        try {
            DateFormat sdf = new SimpleDateFormat(format);
            result = sdf.format(ts);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void main(String[] args) {
        String eval = eval(System.currentTimeMillis(), "yyyyMMddHH");
        System.out.println(eval);
    }
}

