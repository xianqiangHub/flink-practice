package com.bigdata.demoTwo;

import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

/**
 * 也可以使用默认提供的SimpleStringSchema
 */
public class POJOSchema implements DeserializationSchema<POJO>, SerializationSchema<POJO> {

    private static final long serialVersionUID = 1415686761399038954L;

    @Override
    public TypeInformation<POJO> getProducedType() {
        return TypeExtractor.getForClass(POJO.class);
    }

    @Override
    public byte[] serialize(POJO element) {
        return new Gson().toJson(element).getBytes();
    }

    @Override
    public POJO deserialize(byte[] message) throws IOException {
        return new Gson().fromJson(new String(message), POJO.class);
    }

    @Override
    public boolean isEndOfStream(POJO nextElement) {
        return false;
    }


}
