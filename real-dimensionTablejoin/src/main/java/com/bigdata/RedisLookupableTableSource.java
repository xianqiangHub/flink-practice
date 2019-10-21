package com.bigdata;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

//LookupableTableSource 实现维表的流
public class RedisLookupableTableSource implements StreamTableSource<Row>, LookupableTableSource<Row> {

    private final String[] fieldNames;
    private final TypeInformation[] fieldTypes;

    public RedisLookupableTableSource(String[] fieldNames, TypeInformation[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    @Override
    public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
        return null;
    }

    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
        return RedisAsyncTableFunction.Builder.getBuilder().withFieldNames(fieldNames)
                .withFieldTypes(fieldTypes)
                .build();
    }

    @Override
    public boolean isAsyncEnabled() {
        return true;
    }

    //Returns the data of the table as a {@link DataStream}.
    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        throw new UnsupportedOperationException("do not support getDataStream");
    }

    //Returns the schema of the produced table.
    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder().fields(fieldNames, TypeConversions.fromLegacyInfoToDataType(fieldTypes)).build();
    }

    //Returns the {@link DataType} for the produced data of the {@link TableSource}
    @Override
    public DataType getProducedDataType() {
        return TypeConversions.fromLegacyInfoToDataType(new RowTypeInfo(fieldTypes, fieldNames));
    }

    public static final class Builder {
        private String[] fieldNames;
        private TypeInformation[] fieldTypes;

        private Builder() {
        }

        public static Builder newBuilder() {
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

        public RedisLookupableTableSource build() {
            return new RedisLookupableTableSource(fieldNames, fieldTypes);
        }
    }


}
