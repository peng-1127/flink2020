package com.peng.gmall.realtime.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.peng.gmall.realtime.app.func.MyStringDebeziumDeserializationSchema;
import com.peng.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bp
 * @create 2021-06-25 13:01
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        //1.创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //如果读取的是kafka的数据,并行度需要和kafka分区数保持一致
        env.setParallelism(1);

        //2.设置CK&状态后端
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(5000L);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-210108/cdc/ck"));


        //3.使用CDC建立FlinkSource
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-flink")
                .startupOptions(StartupOptions.latest())
                .deserializer(new MyStringDebeziumDeserializationSchema())
                .build();

        //使用CDC Source从MySQL读取数据
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        //4.将数据写入Kafka
        String topic = "ods_base_db";
        streamSource.addSink(MyKafkaUtil.getKafkaProducer(topic));

        streamSource.print();

        //5.启动
        env.execute();
    }
}
