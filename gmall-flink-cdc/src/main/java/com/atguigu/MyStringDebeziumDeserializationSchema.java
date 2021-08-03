package com.atguigu;

import com.alibaba.fastjson.JSONObject;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;


/**
 * @author bp
 * @create 2021-06-25 13:11
 */

/**
 * database:""
 * tablename:""
 * data:""
 * before:""
 * type:""
 */
public class MyStringDebeziumDeserializationSchema implements com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        //1.创建结果JSON
        JSONObject result = new JSONObject();


        //获取topic字段
        String topic = sourceRecord.topic();
        //提取库名和表名
        String[] split = topic.split("\\.");
        String database = split[1];
        String tablename = split[2];
        //提取数据本身
        Struct value = (Struct) sourceRecord.value();
        Struct after = value.getStruct("after");
        JSONObject afterData = new JSONObject();
        if (after != null) {
            Schema schema = after.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                afterData.put(field.name(), after.get(field));
            }
        }

        //提取上一条数据本身
        Struct berfore = value.getStruct("before");
        JSONObject beforeData = new JSONObject();
        if (berfore != null) {
            Schema schema = berfore.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                beforeData.put(field.name(), berfore.get(field));
            }
        }

        //获取类型类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();

        //将类型中create改成inserte
        if ("create".equals(type)) {
            type = "inserte";
        }

        //2.补充字段
        result.put("database", database);
        result.put("tablename", tablename);
        result.put("data", afterData);
        result.put("before", beforeData);
        result.put("type", type);


        //输出数据
        collector.collect(result.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
