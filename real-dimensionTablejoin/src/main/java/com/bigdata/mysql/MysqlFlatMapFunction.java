package com.bigdata.mysql;

import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import scala.Int;

import java.sql.*;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class MysqlFlatMapFunction extends RichFlatMapFunction<AdData, AdData> {

    private AtomicReference<HashMap<Integer, Integer>> atomicReference;

    @Override
    public void open(Configuration parameters) throws Exception {
        atomicReference = new AtomicReference<>();
        //初始化获取MySQL所有数据
        atomicReference.set(loadData());
        //定时新建线程异步获取
        ScheduledExecutorService exectors = Executors.newSingleThreadScheduledExecutor();
        exectors.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                reloadData();
            }
        }, 5, 5, TimeUnit.MINUTES);
    }

    @Override
    public void flatMap(AdData value, Collector<AdData> out) throws Exception {
        int tId = value.gettId();
        Integer aid = atomicReference.get().get(tId);
        AdData data = new AdData(aid, value.gettId(), value.getClientId(), value.getActionType(), value.getTime());
        out.collect(data);
    }

    @Override
    public void close() throws Exception {
        //
    }

    private void reloadData() {
        HashMap<Integer, Integer> newData = loadData();
        atomicReference.set(newData);
    }

    private HashMap<Integer, Integer> loadData() {
        //加载到内存
        HashMap<Integer, Integer> data = new HashMap<>();

        Connection conn = null;
        Statement stmt = null;
        try {
            // 注册 JDBC 驱动
            Class.forName("com.mysql.jdbc.Driver");
            String url = "com.mysql.jdbc.Driver";
            String user = "userName";
            String password = "123456";
            conn = DriverManager.getConnection(url, user, password);

            stmt = conn.createStatement();
            String sql;
            sql = "SELECT aid, tid FROM ads";
            ResultSet rs = stmt.executeQuery(sql);

            // 展开结果集数据库
            while (rs.next()) {
                // 通过字段检索
                int aid = rs.getInt("aid");
                int tid = rs.getInt("tid");
                data.put(tid, aid);
            }
            // 完成后关闭
            rs.close();
            stmt.close();
            conn.close();
        } catch (SQLException se) {
            // 处理 JDBC 错误
            se.printStackTrace();
        } catch (Exception e) {
            // 处理 Class.forName 错误
            e.printStackTrace();
        } finally {
            // 关闭资源
            try {
                if (stmt != null) stmt.close();
            } catch (SQLException se2) {
            }// 什么都不做
            try {
                if (conn != null) conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
        return data;
    }


}
