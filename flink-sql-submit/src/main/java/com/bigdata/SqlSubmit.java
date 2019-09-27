/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bigdata;
import com.bigdata.cli.CliOptions;
import com.bigdata.cli.CliOptionsParser;
import com.bigdata.cli.SqlCommandParser;
import com.bigdata.cli.SqlCommandParser.SqlCommandCall;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableEnvironment;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 *如何使用 Blink Planner
 一个简单的 SqlSubmit 是如何实现的
 如何用 DDL 创建一个 Kafka 源表和 MySQL 结果表
 运行一个从 Kafka 读取数据，计算 PVUV，并写入 MySQL 的作业
 设置调优参数，观察对作业的影响
 */

/**
 * 需求是计算每小时全网的用户访问量，和独立用户数。很多用户可能会想到使用滚动窗口来计算。
 * 但这里我们介绍另一种方式。即 Group Aggregation 的方式。
 *
 * 这种方式的执行模式是每收到一条数据，然后将最新结果输出。所以实时性很高，但输出量也大。
 */

public class SqlSubmit {

    //  CREATE_TABLE:  INSERT_INTO:   SET:
    public static void main(String[] args) throws Exception {
        final CliOptions options = CliOptionsParser.parseClient(args);
        SqlSubmit submit = new SqlSubmit(options);
        submit.run();
    }

    // --------------------------------------------------------------------------------------------

    private String sqlFilePath;
    private String workSpace;
    private TableEnvironment tEnv;

    private SqlSubmit(CliOptions options) {
        this.sqlFilePath = options.getSqlFilePath();
        this.workSpace = options.getWorkingSpace();
    }

    private void run() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()      //思考流式的sql
                .build();
        this.tEnv = TableEnvironment.create(settings);
        List<String> sql = Files.readAllLines(Paths.get(workSpace + "/" + sqlFilePath));
        //都进来的sql封装SqlCommandCall，，多个list 解析核心
        List<SqlCommandCall> calls = SqlCommandParser.parse(sql);
        for (SqlCommandCall call : calls) {
            callCommand(call);
        }
        tEnv.execute("SQL Job");
    }

    // --------------------------------------------------------------------------------------------

    private void callCommand(SqlCommandCall cmdCall) {
        switch (cmdCall.command) {
            case SET:
                callSet(cmdCall);
                break;
            case CREATE_TABLE:
                callCreateTable(cmdCall);
                break;
            case INSERT_INTO:
                callInsertInto(cmdCall);
                break;
            default:
                throw new RuntimeException("Unsupported command: " + cmdCall.command);
        }
    }

    private void callSet(SqlCommandCall cmdCall) {
        String key = cmdCall.operands[0];
        String value = cmdCall.operands[1];
        tEnv.getConfig().getConfiguration().setString(key, value);
    }

    private void callCreateTable(SqlCommandCall cmdCall) {
        String ddl = cmdCall.operands[0];
        try {
            tEnv.sqlUpdate(ddl);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
    }

    private void callInsertInto(SqlCommandCall cmdCall) {
        String dml = cmdCall.operands[0];
        try {
            tEnv.sqlUpdate(dml);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + dml + "\n", e);
        }
    }
}
