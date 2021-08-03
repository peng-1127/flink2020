package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.VisitorStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * @author bp
 * @create 2021-07-02 19:32
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //设置CK和状态后端
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(5000L);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-210108/cdc/ck"));

        //2.读取Kafka主题创建流并转换为json对象
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String groupId = "visitor_stats_app_2010108";
        //读取UV数据
        SingleOutputStreamOperator<JSONObject> uvKafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId))
                .map(JSONObject::parseObject);
        //读取跳出数据
        SingleOutputStreamOperator<JSONObject> ujKAfkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId))
                .map(JSONObject::parseObject);
        //读取page数据
        SingleOutputStreamOperator<JSONObject> pageKafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId))
                .map(JSONObject::parseObject);

        //3.处理数据,让多个流中的数据类型统一,包括维度数据和度量数据
        //uvKafkaDS获取公共字段
        SingleOutputStreamOperator<VisitorStats> visitStatsUvDS = uvKafkaDS.map(json -> {
            JSONObject common = json.getJSONObject("common");
            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    json.getLong("ts"));
        });
        //ujKafkaDS获取公共字段
        SingleOutputStreamOperator<VisitorStats> visitStatsUjDS = uvKafkaDS.map(json -> {
            JSONObject common = json.getJSONObject("common");
            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    json.getLong("ts"));
        });

        //pageKafkaDS
        SingleOutputStreamOperator<VisitorStats> visitorStatsPageDS = pageKafkaDS.map(json -> {

            //获取公共字段
            JSONObject common = json.getJSONObject("common");

            //获取访问时间
            JSONObject page = json.getJSONObject("page");
            //获取持续访问时间
            Long duringTime = page.getLong("during_time");

            //获取进入页面次数sv
            long sv = 0L;
            String last_page_id = page.getString("last_page_id");
            if (last_page_id == null) {
                sv = 1L;
            }

            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 1L, sv, 0L, duringTime,
                    json.getLong("ts"));
        });

        //4.union多个流
        DataStream<VisitorStats> unionDS = visitorStatsPageDS.union(visitStatsUjDS, visitStatsUvDS);
        //5.提取时间戳生成watermark
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWmDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
            @Override
            public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                return element.getTs();
            }
        }));
        //6.keyby分组,开窗,聚合(根据维度数据)
        //根据维度数据keyBy,纵向的进行分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWmDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<>(value.getAr(),
                        value.getCh(),
                        value.getIs_new(),
                        value.getVc());
            }
        });
        //开窗:批次获取数据
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        //聚合:根据4个维度聚合度量值+开始,结束时间+统计时间
        SingleOutputStreamOperator<VisitorStats> result = windowedStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                return new VisitorStats(
                        "",
                        "",
                        value1.getVc(),
                        value1.getCh(),
                        value1.getAr(),
                        value1.getIs_new(),
                        value1.getUv_ct() + value2.getUv_ct(),
                        value1.getPv_ct() + value2.getPv_ct(),
                        value1.getSv_ct() + value2.getSv_ct(),
                        value1.getUj_ct() + value2.getUj_ct(),
                        value1.getDur_sum() + value2.getDur_sum(),
                        value2.getTs());
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                //获取窗口开始时间,结束时间,目的是看这个时间是在哪个批次的时间段
                long start = window.getStart();
                long end = window.getEnd();
                //获取聚合后的数据(只要前面做了聚合,这只有一条数据)
                VisitorStats visitorStats = input.iterator().next();
                //补充开始和结束字段,将时间戳改为日期格式
                visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(start)));

                visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(end)));

                out.collect(visitorStats);

            }
        });
        //7.聚合好的数据写入ClickHouse

        result.print(">>>>>>>>>>>>>>>>>>>>>>>>");
        result.addSink(ClickHouseUtil.getJdbcSink("insert into visitor_stats_0108 values(?,?,?,?,?,?,?,?,?,?,?,?)"));


        //8.启动任务
        env.execute();
    }
}
