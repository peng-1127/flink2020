package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.bean.TransientSink;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author bp
 * @create 2021-07-06 18:30
 */
public class ClickHouseUtil {
    public static <T> SinkFunction<T> getJdbcSink(String sql) {

        //参数:d
        // 1.sql(占位符)
        // 2.给占位符赋值
        // 3.要不要批量提交
        // 4.drive,url. 用户名,密码(clickHouse不需要)
        return JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        //保证???的顺序和javaBean的字段顺序一致
                        //通过反射的方式获取所有列信息
                        //This includes public, protected, default(package) access, and private fields, but excludes inherited fields.
                        Field[] fields = t.getClass().getDeclaredFields();

                        int offset = 0;
                        //遍历属性,获取每个属性对应的值信息
                        for (int i = 0; i < fields.length; i++) {
                            //获取列
                            Field field = fields[i];

                            //获取该列上是否存在不需要写出注解
                            TransientSink annotation = field.getAnnotation(TransientSink.class);
                            if (annotation != null) {
                                //如果用到跳过的注解,那么给预编译sql加一位
                                offset++;
                                continue;
                            }

                            //设置私有属性可访问
                            field.setAccessible(true);
                            //获取值
                            try {
                                Object value = field.get(t);
                                //给预编译sql赋值
                                preparedStatement.setObject(i + 1 - offset, value);

                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }
}
