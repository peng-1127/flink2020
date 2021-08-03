package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.DimUtil;
import com.atguigu.gmall.realtime.utils.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author bp
 * @create 2021-07-01 0:23
 */

//泛型:输入:OrderWide,输出:OrderWide,但是换个数据流就用不了了
public abstract class AsyncDimFunction<T> extends RichAsyncFunction<T, T> implements AsyncJionFunction<T> {

    //定义pheonix连接
    private Connection connection;
    //定义线程池
    private ThreadPoolExecutor threadPoolExecutor;
    //定义表名
    private String tableName;

    public AsyncDimFunction(String tableName) {
        this.tableName = tableName;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化pheonix连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        //初始化线程池
        threadPoolExecutor = ThreadPoolUtil.getInstance();
    }

    //input:数据流中的数据
    //ResultFuture:返回值
    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        //开启线程池,提交任务
        threadPoolExecutor.submit(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                //1.查询维度数据,通过连接,表名和主键就可以查到这一条信息
                String key = getKey(input);
                JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, key);

                //2.将消费数据与源数据结合(如:将用户信息补充到javabean的字段中)
                if (dimInfo != null) {
                    join(input, dimInfo);
                }
                //3.将结合好的数据写出
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }


    //请求数据库的超时时间
    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        //打印超时数据,一旦发生需要检查维度表中是否在查询的信息
        System.out.println("TimeOut" + input);
    }
}
