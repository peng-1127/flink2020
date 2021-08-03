package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.*;
import java.util.*;

/**
 * 参数:
 * IN1:主流
 * IN2:广播流
 * OUT:输出
 * @author Programmer
 */
public class TableProcessFuntion extends BroadcastProcessFunction<JSONObject, String, JSONObject> {


    private OutputTag<JSONObject> outputTag;
    private Connection connection;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    //将侧输出流的方法传出去
    public TableProcessFuntion(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //加载驱动
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        //URL
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //广播流
    //value:{"database":"","tableName":"","type":"","data":{"source_table":"base_trademark",...},"before":{"id":"1001",...}}
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        //TODO 1.获取并解析成javaBean
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("data"), TableProcess.class);

        //TODO 2.检验表是否存在,不存在则需要在phoenix中建表
        //如果将字段都拿进来,kafka的主题也会去建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            //四个参数:表名,字段,主键,扩展字段
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }

        //TODO 3.写入状态,广播出去
        //获取状态(上下文)
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        //广播往里面放数据,k:表名+操作类型(唯一判断),v:javabean
        String key = tableProcess.getSourceTable() + "_" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);

    }

    //主流
    //value:{"database":"","tableName":"","type":"","data":{"id":"1",...},"before":{"id":"1001",...}}
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //1.读到广播流的状态(获取广播过来的配置信息)
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //拿到key,用key获取value
        String key = value.getString("tableName") + "_" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        //2.根据状态(配置信息),过滤字段
        if (tableProcess != null) {
            //id,tm_name,logo , id,tm_name
            filterColumn(value.getJSONObject("data"), tableProcess.getSinkColumns());

            //3.分流(kafka,hbase)
            //将sinkTable:如dim_base_trademark字段添加到数据中
            value.put("sinkTable", tableProcess.getSinkTable());
            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                //将数据写入侧输出流
                ctx.output(outputTag, value);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                out.collect(value);
            }
        } else {
            //表里面已经指定好操作类型了,不存在的在生产环境中可能放在log日志里
            System.out.println("该组合Key:" + key + "不存在配置信息!");
        }
    }

    //============主流过滤方法===========================
    //根据配置信息过滤数据 "data":{"id":"1","tm_name":"atguigu","logo":"xx"}
    //参数:原先的字段;过滤后的字段
    private void filterColumn(JSONObject data, String sinkColumns) {
        //遍历id,tm_name,logo字段,将id,tm_name字段变成集合,看id,tm_name字段包不包含id,tm_name,logo字段,如果不包含,就删掉
        //1.将需要保留的字段切分
        String[] columnsArr = sinkColumns.split(",");
        //2.将数组变成集合
        List<String> columnList = Arrays.asList(columnsArr);
        //3.遍历data数据中的列信息
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            //4.判断配置表的字段中有没有原数据字段
            //entry是个k,v组合
            if (!columnList.contains(next.getKey())) {
                iterator.remove();
            }
        }
    }

    //============广播流建表方法=========================
    //在Pheonix中建表
    // create table if not exists
    //      库.表(aa varchar primary key ,bb varchar)
    //参数:表名,字段,主键,扩展
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement preparedStatement = null;

        //处理主键和扩展字段,给定默认值(因为不给的话默认值是null)
        try {
            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }
            //1.拼接sql
            StringBuilder createSql = new StringBuilder("create table if not exists ");
            createSql.append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] columns = sinkColumns.split(",");
            //遍历columns
            //用fori循环更好,因为需要判断最后一个没有逗号
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                //判断当前的列是否为主键
                if (sinkPk.equals(column)) {
                    createSql.append(column).append(" varchar primary key ");
                } else {
                    createSql.append(column).append(" varchar ");
                }
                //如果当前字段不是最后一个字段,则需要添加","
                if (i < columns.length - 1) {
                    createSql.append(",");
                }
            }
            //拼接扩展字段
            createSql.append(")").append(sinkExtend);

            //打印建表语句
            System.out.println(createSql.toString());

            //2.编译sql
            preparedStatement = connection.prepareStatement(createSql.toString());

            //3.执行,建表
            preparedStatement.execute();

        } catch (SQLException e) {
            //如果建表不成功,要停止
            throw new RuntimeException("Pheonix创建表-----" + sinkTable + "-----失败!");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
