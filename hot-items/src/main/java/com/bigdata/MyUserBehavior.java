package com.bigdata;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.Charset;

public class MyUserBehavior implements DeserializationSchema<UserBehavior>, SerializationSchema<UserBehavior> {

    private static final UserBehavior userBehavior = new UserBehavior();

    @Override
    public UserBehavior deserialize(byte[] message) throws IOException {

        String[] split = new String(message).split(",");
        userBehavior.setUserId(Long.valueOf(split[0]));
        userBehavior.setItemId(Long.valueOf(split[1]));
        userBehavior.setCategoryId(Integer.valueOf(split[2]));
        userBehavior.setBehavior(split[3]);
        userBehavior.setTimestamp(Long.valueOf(split[4]));
        return userBehavior;
    }

    @Override
    public boolean isEndOfStream(UserBehavior nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(UserBehavior element) {
        return element.toString().getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<UserBehavior> getProducedType() {
        return TypeInformation.of(UserBehavior.class);
    }
}
