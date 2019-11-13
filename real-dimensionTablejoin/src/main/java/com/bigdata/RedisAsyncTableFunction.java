package com.bigdata;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class RedisAsyncTableFunction extends AsyncTableFunction<Row> {
    private final String[] fieldNames;
    private final TypeInformation[] fieldTypes;

    //transient 短暂的不序列话
    //线程安全的(多个客户端一起使用)，你可以不加 transient 关键字,初始化一次。
    // 加上 transient,在 open 方法中，为每个 Task 实例初始化一个。
//    异步外部数据源的client要在类中定义为 transient,然后在 open 方法中进行初始化，
// 这样每个任务实例都会有一个外部数据源的 client。防止同一个 client 多个任务实例调用，出现线程不安全情况。
    private transient RedisAsyncCommands<String, String> async;

    public RedisAsyncTableFunction(String[] fieldNames, TypeInformation[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    /**
     * 使用异步函数访问外部数据系统，一般是外部系统有异步访问客户端
     * 如果没有的话，可以自己使用线程池异步访问外部系统。
     * 至于为什么使用异步访问函数，无非就是为了提高程序的吞吐量，
     * 不需要每条记录访问返回数据后，才去处理下一条记录。
     */
    @Override
    public void open(FunctionContext context) {
        //配置redis异步连接
        /**
         * Lettuce和Jedis的都是连接Redis Server的客户端程序。
         * Jedis在实现上是直连redis server，多线程环境下非线程安全，除非使用连接池，为每个Jedis实例增加物理连接。
         * Lettuce基于Netty的连接实例（StatefulRedisConnection），可以在多个线程间并发访问，且线程安全，
         * 满足多线程环境下的并发访问，同时它是可伸缩的设计，一个连接实例不够的情况也可以按需增加连接实例
         */
        RedisClient redisClient = RedisClient.create("redis://172.16.44.28:6379/0");
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        async = connection.async();
    }

    //每一条流数据都会调用此方法进行join
    // implement an "eval" method with as many parameters as you want
    public void eval(CompletableFuture<Collection<Row>> future, Object... paramas) {
        //表名、主键名、主键值、列名
        String[] info = {"userInfo", "userId", paramas[0].toString(), "userName"};
        String key = String.join(":", info);
        RedisFuture<String> redisFuture = async.get(key);

        redisFuture.thenAccept(new Consumer<String>() {
            @Override
            public void accept(String value) {
                future.complete(Collections.singletonList(Row.of(key, value)));
                //todo
//                BinaryRow row = new BinaryRow(2);
            }
        });
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    public static final class Builder {
        private String[] fieldNames;
        private TypeInformation[] fieldTypes;

        private Builder() {
        }

        public static Builder getBuilder() {
            return new Builder();
        }

        public Builder withFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder withFieldTypes(TypeInformation[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        public RedisAsyncTableFunction build() {
            return new RedisAsyncTableFunction(fieldNames, fieldTypes);
        }
    }

}
