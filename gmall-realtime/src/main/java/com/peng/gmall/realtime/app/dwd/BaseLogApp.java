package com.peng.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.peng.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author bp
 * @create 2021-06-26 1:44
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //TODO 2.读取kafka ods_base_log 主题数据创建流 并转换成JSON对象
        String sourceTopic = "ods_base_log";
        String groupId = "ods_base_log_210108";

        //定义侧输出流
        OutputTag<String> dirtyData = new OutputTag<String>("DirtyData") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId))
                //转换成JSON格式,处理脏数据,脏数据放在侧输出流里
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                        //Ctrl+Alt+T 进行try...catch
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);
                            out.collect(jsonObject);
                        } catch (Exception e) {
                            //解析失败
                            ctx.output(dirtyData, value);

                        }


                    }
                });
        //测试
//        jsonObjectDS.print("Main>>>>>>>>>>>");
        jsonObjectDS.getSideOutput(dirtyData).print("DirtyData>>>>>>>>>>>");
        //TODO 3.按照mid分组
        KeyedStream<JSONObject, String> midKeyedStream = jsonObjectDS.keyBy(data -> data.getJSONObject("common").getString("mid"));
        //TODO 4.新老用户校验
        SingleOutputStreamOperator<JSONObject> jsonObjectWithNewFlagDS = midKeyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {

                //定义value状态
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("is-new", String.class));

            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                //获取新老用户
                String isNew = value.getJSONObject("common").getString("is_new");

                //判断前台校验为新用户
                if ("1".equals(isNew)) {
                    //取出状态里的数据
                    String state = valueState.value();

                    //判断状态是否有数据
                    if (state != null) {
                        //将传进来的数据进行修改
                        value.getJSONObject("common").put("is_new", "0");

                    } else {
                        //将状态更新
                        valueState.update("0");
                    }
                }

                return value;
            }
        });

        //打印测试
        jsonObjectWithNewFlagDS.print();

        //TODO 5.使用processAPI中侧输出流进行分流处理

        //定义start侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };

        //定义display侧输出流
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };


        SingleOutputStreamOperator<String> pageDS = jsonObjectWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                //获取strat数据
                String start = value.getString("start");
                //判断start数据不为null或者长度大于0,写到侧输出流
                if (start != null && start.length() > 0) {
                    //将start写入侧输出流
                    ctx.output(startTag, value.toJSONString());
                } else {
                    //不是start,就是page,将page写入主流
                    out.collect(value.toJSONString());

                    //获取dispalys数据

                    JSONArray displays = value.getJSONArray("displays");

                    //判断dispalys是不是null,不是null再做其他判断
                    if (displays != null && displays.size() > 0) {
                        //遍历曝光数据,将单条数据取出来,写到侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            //displays是个json,套在common的json里面
                            JSONObject jsonObject = displays.getJSONObject(i);
                            //将页面信息放在曝光日志中
                            String pageId = value.getJSONObject("page").getString("page_id");
                            jsonObject.put("page_id", pageId);

                            //将数据写出
                            ctx.output(displayTag, jsonObject.toJSONString());
                        }

                    }

                }

            }
        });
        //TODO 6.将不同流的数据写入kafka主题中

        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        //测试
        pageDS.print("Page>>>>>>>>>>>>");
        startDS.print("Start>>>>>>>>>>>");
        displayDS.print("Display>>>>>>>>");

        //写入kafka
        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));


        //TODO 7.启动
        env.execute();

    }
}
