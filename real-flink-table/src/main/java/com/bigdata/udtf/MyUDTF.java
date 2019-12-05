package com.bigdata.udtf;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * 列转行
 * <p>
 * hive中操作为
 * select movie,category_name from
 * movie_info lateral view explode(category) table_tmp as category_name;
 *
 * 和hive一样为calcite解析sql
 */
public class MyUDTF extends TableFunction<Row> {

    //    必须声明为public/not static
    public void eval(String s) {
        JSONArray jsonArray = JSONArray.parseArray(s);
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            String type = jsonObject.getString("type");
            String value = jsonObject.getString("value");
            collector.collect(Row.of(type, value));
        }
    }

    //    其中T表示返回的数据类型，通常如果是原子类型则直接指定例如String, 如果是复合类型通常会选择Row,
//    FlinkSQL 通过类型提取可以自动识别返回的类型，如果识别不了需要重载其getResultType方法，
//    指定其返回的TypeInformation
    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.STRING(), Types.STRING());
    }
}
