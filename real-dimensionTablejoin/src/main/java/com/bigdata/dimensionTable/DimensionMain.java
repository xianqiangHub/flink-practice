package com.bigdata.dimensionTable;

import com.bigdata.RedisLookupableTableSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCTableSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class DimensionMain {

    public static void main(String[] args) throws Exception {
        //blink 通过lookup able table source 实现维表jion
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

//        TypeInformation[] types = new TypeInformation[]{Types.STRING, Types.STRING, Types.LONG};
//        String[] fields = new String[]{"id", "user_click", "time"};
//        RowTypeInfo typeInformation = new RowTypeInfo(types, fields);
//        RedisLookupableTableSource tableSource  = RedisLookupableTableSource.
//                Builder.
//                newBuilder().
//                withFieldNames(new String[]{"id", "name"})
//                .withFieldTypes(new TypeInformation[]{Types.STRING, Types.STRING})
//                .build();

        JDBCTableSource tableSource = JDBCTableSource.builder().build();
        //维表读进来是个tablesource
        tableEnv.registerTableSource("jdbc",tableSource);
//        tableEnv.registerDataStream();

        env.execute("jdbc");
    }
}
