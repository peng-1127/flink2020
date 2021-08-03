package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * @author bp
 * @create 2021-07-01 1:46
 */
public interface AsyncJionFunction<T> {
    //定义抽象方法,用外界的id,传进来,谁用到这个函数,谁去重写
    public abstract String getKey(T input);

    //定义抽象方法,input为传进来的流,dimInfo为查询的维度数据
    public abstract void join(T input, JSONObject dimInfo) throws ParseException;
}
