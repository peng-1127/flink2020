package com.atguigu.gmall.realtime.common;

/**
 * @author bp
 * @create 2021-06-27 14:41
 */
public class GmallConfig {

    //Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL_REALTIME";

    //Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    //clickHouse连接
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/default";

    //clickHouse驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
}
