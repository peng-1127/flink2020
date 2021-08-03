package com.peng;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bp
 * @create 2021-06-24 22:44
 */
public class FlinkDataStream02 {
    public static void main(String[] args) throws Exception {
        //1.获取流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //2.使用CDC建立FlinkSource
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall-flink")
                .username("root")
                .password("123456")
                .startupOptions(StartupOptions.initial())  //第一次启动的时候做了一次快照,然后再从binlog最新的开始读
                .deserializer(new MyStringDebeziumDeserializationSchema())
                .tableList("gmall-flink.base_trademark")
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);
        //3.打印
        dataStreamSource.print();
        //4.启动
        env.execute();
    }
}
