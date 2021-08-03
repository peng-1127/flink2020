package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.app.func.DimSink;
import com.atguigu.gmall.realtime.app.func.MyStringDebeziumDeserializationSchema;
import com.atguigu.gmall.realtime.app.func.TableProcessFuntion;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author bp
 * @create 2021-06-26 20:17
 */


//数据流: web/app -> niginx -> 业务服务器 -> mysql -> FlinkApp -> ods -> FlinkApp -> kafka/pheonix
//程序:                         mock   -> mysql -> FlinkCDC ->kafka/zk -> BaseDbApp -> kafka/pheonix
// ->Hbase/hdfs
public class BaseDbApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //checkpoint

        //2.读取kafka ods_base_db 主题数据并转换为jsonObject
        String topic = "ods_base_db";
        String groupId = "ods_base_db_210108";
        SingleOutputStreamOperator<JSONObject> jsonObjDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId)).map(JSON::parseObject);

        //3.过滤空值数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String data = value.getString("data");
                if (data == null || data.length() <= 2) {
                    return false;
                } else {
                    return true;
                }
            }

            ;
        });
        //打印测试
        //filterDS.print();

        //4.使用flinkCDC读取配置信息表并将该流转换成广播流
        //FlinkCDC读取mysql配置信息,刚读进来的时候用自定义序列化的json字符串
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-realtime")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyStringDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> tableProcessDS = env.addSource(sourceFunction);

        //将读进来的流变成广播流
        //广播往里面放数据,k:表名+操作类型(唯一判断),v:javabean
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessDS.broadcast(mapStateDescriptor);

        //5.将主流与广播流进行连接
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastStream);

        //6.根据广播流中发送来的数据将主流分为kafka事实数据流和hbase维度数据流
        //定义侧输出流
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {
        };
        SingleOutputStreamOperator<JSONObject> kafkaDS = connectedStream.process(new TableProcessFuntion(hbaseTag, mapStateDescriptor));

        //打印测试
        kafkaDS.print("Kafka>>>>>>>>>>");
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);
        hbaseDS.print("Hbase>>>>>>>>>>");


        //7.将hbase数据写入phoenix
        hbaseDS.addSink(new DimSink());

        //8.将kafka数据写入kafka
        kafkaDS.addSink(MyKafkaUtil.getKafkaProducerWithSchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<>(jsonObject.getString("sinkTable"),
                        jsonObject.getString("data").getBytes());
            }
        }));

        //9.启动
        env.execute();

    }
}
