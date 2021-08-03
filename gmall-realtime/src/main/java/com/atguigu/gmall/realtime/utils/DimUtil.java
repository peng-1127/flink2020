package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * @author bp
 * @create 2021-06-30 18:45
 */

//此类是为了对jdbc工具类中的sql再进行封装,并采用redis缓存
public class DimUtil {
    //参数为连接,表名和id,因为用表名和id(主键)就可以定位到这条数据
    public static JSONObject getDimInfo(Connection connection, String tableName, String id) {

        //TODO 先查询Redis
        //获取redis连接
        Jedis jedis = RedisUtil.getJedis();
        //=========================================
        /*获取Redis三件事
        存什么?JSONString
        选择什么类型?String(不用map是因为map的大key是table,要设置过期时间的话整个表都会过期,会产生缓存雪崩:到时间所有的key都失效)
        RedisKey设计? table+id (防止数据倾斜,将key打散)*/
        //=========================================

        //获取redis key
        String redisKey = "DIM:" + tableName + ":" + id;
        //获取数据
        String jsonStr = jedis.get(redisKey);
        //如果查到了,关闭连接,返回数据
        if (jsonStr != null) {
            //重置过期时间
            jedis.expire(redisKey, 60 * 60 * 24);
            //将redis关闭,还回连接池
            jedis.close();
            return JSONObject.parseObject(jsonStr);
        }

        //TODO 如果redis里面没有数据,那么拼接SQL
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id = '" + id + "'";
        System.out.println(querySql);
        //查询数据获取返回值
        //参数:
        //connection:外面传进来的连接
        //querySql:查询语句
        //JSONObject.class:输出为JSONObeject类型,方便后续处理
        //false:不是下划线转换为驼峰命名方式
        List<JSONObject> queryList = JdbcUtils.query(connection, querySql, JSONObject.class, false);

        //TODO 同时将数据写入Redis
        JSONObject jsonObject = queryList.get(0);
        jedis.set(redisKey, jsonObject.toJSONString());
        //设置过期时间
        jedis.expire(redisKey, 60 * 60 * 24);
        //将redis关闭,还回连接池
        jedis.close();

        //返回类型,因为List中只有一条数据
        return jsonObject;

    }

    //如果数据类型为更新,hbase中和redis中数据不一致,所以定义删除方法,将redis中的数据删除
    public static void delRedisDimInfo(String tableName, String id) {
        //拼接RedisKey
        String redisKey = "DIM:" + tableName + ":" + id;
        //删除数据
        Jedis jedis = RedisUtil.getJedis();
        jedis.del(redisKey);
        //归还数据
        jedis.close();

    }
    //====================================================

    //测试
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        //获取连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        long start = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_BASE_CATEGORY1", "1001"));
        long second = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_BASE_CATEGORY1", "1001"));
        long end = System.currentTimeMillis();

        //测试两条sql中间耗时
        System.out.println(second - start);
        System.out.println(end - second);
    }
}
