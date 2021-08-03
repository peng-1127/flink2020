package com.peng;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


/**
 * @author bp
 * @create 2021-06-26 0:21
 */
public class FlinkSQL {
    public static void main(String[] args) throws Exception {
        //1.获取流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //2.获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.创建FlinkSql方式读取MySql变化数据
        tableEnv.executeSql("CREATE TABLE base_trademark ( " +
                " id String, " +
                " tm_name STRING,  " +
                " logo_url STRING  " +
                ") WITH ( " +
                " 'connector' = 'mysql-cdc',  " +
                " 'hostname' = 'hadoop102',  " +
                " 'port' = '3306',  " +
                " 'username' = 'root',  " +
                " 'password' = '123456',  " +
                " 'database-name' = 'gmall-flink',  " +
                " 'table-name' = 'base_trademark'  " +
                ")");

        //4.打印
        Table table = tableEnv.sqlQuery("select * from base_trademark");
        tableEnv.toRetractStream(table, Row.class).print();

        //5.启动
        env.execute();
    }
}
