package com.atguigu.gmall.realtime.utils;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author bp
 * @create 2021-06-29 23:25
 */
public class JdbcUtils {
    //参数:
    // 创建的连接
    // sql查询语句
    // 给泛型赋值(要封装的类型)
    // 命名方式是否需要修改:如:mysql中是_命名,javabean中格式小驼峰命名
    public static <T> List<T> query(Connection connection, String sql, Class<T> cls, Boolean underScoreToCamel) {

        //创建结果集合
        ArrayList<T> result = new ArrayList<>();

        //执行查询,把查询的结果一个一个放在创建的结果集合里面
        try {
            //TODO 编译SQL
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            //TODO 执行查询
            ResultSet resultSet = preparedStatement.executeQuery();

            //TODO 解析结果
            //先获取列的次数,只有知道列的次数,才能对列进行遍历
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            //对查询结果的行进行遍历
            while (resultSet.next()) {

                //TODO 获取T对象,每一行数据为一个T对象
                T t = cls.newInstance();

                //对列进行遍历(mysql的索引是从1开始,所以<=)
                for (int i = 1; i <= columnCount; i++) {

                    //TODO 获取列名:要将数据写入class对象里面,光有值不行,还需要列名
                    String columnName = metaData.getColumnName(i);

                    //将列名的命名方式做修改:如果制定需要下划线转换为驼峰命名方式
                    if (underScoreToCamel) {
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    //TODO 获取数据
                    Object value = resultSet.getObject(i);

                    //TODO 给T对象属性赋值,给当前的每一行数据t对象的列名赋值上数据
                    BeanUtils.setProperty(t, columnName, value);
                }

                //TODO 将T对象添加至集合
                result.add(t);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        //返回集合
        return result;
    }

    //TODO 测试:
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        //TODO 1.mysql测试
        //获取连接
       /*Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall-realtime",
                "root",
                "123456");
        //调用工具类
        List<TableProcess> infoList = query(connection, "select * from table_process", TableProcess.class, true);
        //遍历工具类中查询出的数据
        for (TableProcess tableProcess : infoList) {
            System.out.println(tableProcess);
        }
        //关闭连接
        connection.close();*/


        //TODO 2.测试pheonix
        //获取连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        //调用工具类,如果没有javabean,可以用JSONObject
        List<JSONObject> infoList = query(connection,
                "select * from GMALL_REALTIME.DIM_BASE_CATEGORY1",
                JSONObject.class,
                false);

        //遍历工具类中查询出的数据
        for (JSONObject orderInfo : infoList) {
            System.out.println(orderInfo);
        }
        //关闭连接
        connection.close();
    }
}
