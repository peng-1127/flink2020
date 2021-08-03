package com.peng.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.peng.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author bp
 * @create 2021-06-29 18:02
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //1.创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //2.设置CK和状态后端
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(5000L);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-210108/cdc/ck"));

        //3.读kafka数据创建流并转换为JSONObject
        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp_210108";
        String sinkTopic = "dwm_user_jump_detail";
        SingleOutputStreamOperator<JSONObject> jsonObjDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId))
                .map(JSONObject::parseObject);

        //4.提取事件时间生成WaterMark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWMDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        //5.按照mid进行分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWMDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        //6.定义模式序列

        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPage = value.getJSONObject("page").getString("last_page_id");
                        return lastPage == null || lastPage.length() <= 0;
                    }
                }).next("next")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPage = value.getJSONObject("page").getString("last_page_id");
                        return lastPage == null || lastPage.length() <= 0;
                    }
                }).within(Time.seconds(10));

        //7.将模式序列作用在流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //8.提取事件,匹配上的和超时时间都需要提取
        OutputTag<JSONObject> timeOut = new OutputTag<JSONObject>("TimeOut") {
        };  //将超时的数据先放在侧输出流
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(timeOut, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            //提取超时事件(超过10秒后的数据)
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                List<JSONObject> startList = map.get("start");
                return startList.get(0);
            }
            //提取匹配上的事件(start的last_page_id为null,next的last_page_id也为null,并且没有超过10秒,只要第一条数据)
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                List<JSONObject> startList = map.get("start");
                return startList.get(0);
            }
        });

        //9.将数据写入kafka

        //超时数据在侧输出流里
        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(timeOut);
        //两条流进行union
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);
        unionDS.print("unionDS>>>>>>>>>>>>");
        //JsonObject -> toJSONString,写入kafka
        unionDS.map(data -> data.toJSONString()).addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));
        //10.启动
        env.execute();

    }
}
