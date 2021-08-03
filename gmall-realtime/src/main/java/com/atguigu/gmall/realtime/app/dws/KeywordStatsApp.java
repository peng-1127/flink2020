package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.func.SplitKeywordUDTF;
import com.atguigu.gmall.realtime.bean.KeywordStats;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author bp
 * @create 2021-07-07 20:13
 */

//数据流:web/app -> Nginx -> 日志服务器 -> Kafka(ods) -> FlinkApp -> Kafka(dwd) -> FlinkApp -> ClickHouse
//程  序:mock    -> Nginx -> Logger.sh -> Kafka(ZK)  -> BaseLogApp -> Kafka -> KeywordStatsApp -> ClickHouse
public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //如果读取的是Kafka中数据,则需要与Kafka的分区数保持一致
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //设置CK & 状态后端
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(5000L);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-210108/cdc/ck"));

        //TODO 2.使用DDL方式从Kafka读取数据 dwd_page_log,事件时间
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        tableEnv.executeSql("create table page_log(" +
                "   `common` MAP<STRING,STRING>, " +
                "   `page` MAP<STRING,STRING>, " +
                "   `ts` BIGINT, " +
                "   `rowtime` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +  //输出的结果是年月日时分秒,但是ts本身是BIGINT类型,需要加工成年月日时分秒
                "   WATERMARK FOR rowtime AS rowtime " +
                ") with" + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId));
        //TODO 3.过滤出搜索的数据
        Table fullwordTable = tableEnv.sqlQuery("" +
                "select " +
                "   page['item'] fullword, " + //搜索关键词
                "   ts, " +
                "   rowtime " + //开窗要用到
                "from page_log " +
                "where page['last_page_id'] = 'search' " +
                "and page['item_type'] = 'keyword' " +
                "and page['item'] is not null"); //搜索框如果什么都没有输入,也需要过滤掉

        //TODO 4.注册UDTF,将搜索的关键词进行分词处理
        //4.1注册函数,函数名称为split_word
        tableEnv.createTemporarySystemFunction("split_word", SplitKeywordUDTF.class);
        //4.2处理分词:炸裂fullword
        Table splitWordTable = tableEnv.sqlQuery("SELECT word,ts,rowtime " +
                "FROM " + fullwordTable + ", LATERAL TABLE(split_word(fullword))");

        //TODO 5.分组、开窗、聚合
        Table result = tableEnv.sqlQuery("select " +
                "'" + GmallConstant.KEYWORD_SEARCH + "' source," + //只是单纯加了一个"SEARCH"字段
                "   DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " + //窗口的开始时间
                "   DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " + //窗口的结束时间
                "   word keyword, " +
                "   count(*) ct, " + //10秒窗口内聚合的次数
                "   max(ts) ts " +
                "from " + splitWordTable + " " +
                "group by TUMBLE(rowtime, INTERVAL '10' SECOND),word"); //开窗时间
        //TODO 6.将动态表转换为流
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(result, KeywordStats.class);
        //TODO 7.将数据写入ClickHouse
        keywordStatsDataStream.print(">>>>>>>>>>>>>>");
        keywordStatsDataStream.addSink(ClickHouseUtil.<KeywordStats>getJdbcSink(
                "insert into keyword_stats_0108(keyword,ct,source,stt,edt,ts)  " +
                        " values(?,?,?,?,?,?)"));
        //TODO 8.启动
        env.execute();
    }
}
