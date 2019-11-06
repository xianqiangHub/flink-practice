package com.bigdata.exercise;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PostgresqlSource extends RichSourceFunction<String> {

    private static final long serialVersionUID = 1L;

    private Connection connection;

    private boolean isRunning = true;

    private PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String driver = "";
        String url = "";
        String user = "";
        String password = "";

        Class.forName(driver);
        connection = DriverManager.getConnection(url,user, password);
        String sql = " SELECT code,name FROM  public.config ";
        preparedStatement = connection.prepareStatement(sql);
    }


    @Override
    public void run(SourceContext <String> sourceContext) throws Exception {
        while (isRunning) {
            try {
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    Word word = new Word();
                    word.setCode(resultSet.getString("code"));
                    word.setName(resultSet.getString("name"));
                    sourceContext.collect(String.valueOf(word));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            Thread.sleep(3000);
        }
    }



    @Override
    public void cancel() {
        isRunning=false;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (preparedStatement != null) {
            preparedStatement.close();
        }
    }

    private class Word {
        private String code;
        private String name;

        public String getCode() {
            return code;
        }

        public void setCode(String code) {
            this.code = code;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;

        }

        public Word(){
            this.code = code;
            this.name = name;
        }

        @Override
        public String toString() {
            return code+"\t"+name;
        }
    }
}