package com.peng.gmall.realtime.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author bp
 * @create 2021-07-06 18:59
 */

//作用于:作用在字段上
@Target(ElementType.FIELD)
//生效时间:运行时生效
@Retention(RetentionPolicy.RUNTIME)
public @interface TransientSink {
}
