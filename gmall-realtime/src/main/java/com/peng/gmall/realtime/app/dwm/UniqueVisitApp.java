package com.peng.gmall.realtime.app.dwm;


import com.alibaba.fastjson.JSONObject;
import com.peng.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;


/**
 * @author bp
 * @create 2021-06-28 20:06
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //1.获取流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //设置状态后端
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(5000L);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-210108/cdc/ck"));

        //2.读取kafka数据并转换为JSON
        String groupId = "unique_visit_app_210108";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";

        SingleOutputStreamOperator<JSONObject> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId))
                .map(line -> JSONObject.parseObject(line));
        //3.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = kafkaDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        //4.过滤
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            //定义一个value状态
            private ValueState<String> valueState;
            //时间格式化对象
            private SimpleDateFormat sdf;

            //初始化
            @Override
            public void open(Configuration parameters) throws Exception {
                //定义value的描述器
                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("last-visit", String.class);

                // 状态无效时清理,放置状态数据量过大 TTL,如果是晚上12点登录,那么保存24小时,如果是早上8点登录,状态也保留24小时,但是又会根据日期"ts"的判断,将状态截断
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
                stringValueStateDescriptor.enableTimeToLive(ttlConfig);
                //定义value状态,将描述器放在状态里
                valueState = getRuntimeContext().getState(stringValueStateDescriptor);
                //时间格式化
                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //获取上一页信息
                String lastPage = value.getJSONObject("page").getString("last_page_id");
                //判断上一页信息是否存在
                if (lastPage == null || lastPage.length() <= 0) {
                    //先获取状态
                    String lastDate = valueState.value();
                    String date = sdf.format(value.getLong("ts"));

                    //如果状态里面的数据是null或者状态的日期不是当天日期
                    if (lastDate == null || !lastDate.equals(date)) {
                        //将今天的时间写入状态
                        valueState.update(date);

                        return true;
                    } else {
                        return false;
                    }


                } else {
                    return false;
                }


            }
        });


        //5.写入kafka主题
        filterDS.print(">>>>>>>>>>>>>");
        filterDS.map(line -> JSONObject.toJSONString(line)).addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        //6.启动
        env.execute();
    }
}
