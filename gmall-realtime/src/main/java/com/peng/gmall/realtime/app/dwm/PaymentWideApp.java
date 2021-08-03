package com.peng.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.peng.gmall.realtime.bean.OrderWide;
import com.peng.gmall.realtime.bean.PaymentInfo;
import com.peng.gmall.realtime.bean.PaymentWide;
import com.peng.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @author bp
 * @create 2021-07-02 18:08
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //1.获取流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //设置CK和状态后端
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(5000L);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-210108/cdc/ck"));

        //2.读取kafka主题数据并转换为javabean
        String groupId = "payment_wide_group_210108";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentSinkTopic = "dwm_payment_wide";
        //读取支付表数据并转换成javabean类型
        SingleOutputStreamOperator<PaymentInfo> paymentKafakaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId))
                .map(data -> JSONObject.parseObject(data, PaymentInfo.class));
        //读取订单宽表数据并转换成javabean类型
        SingleOutputStreamOperator<OrderWide> orderWideKafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId))
                .map(data -> JSONObject.parseObject(data, OrderWide.class));
        //3.获取watermark
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentKafakaDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return sdf.parse(element.getCreate_time()).getTime();
                        } catch (ParseException e) {

                            return recordTimestamp;
                        }
                    }
                }));

        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideKafkaDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderWide>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide element, long recordTimestamp) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return sdf.parse(element.getCreate_time()).getTime();
                        } catch (ParseException e) {

                            return recordTimestamp;
                        }
                    }
                }));

        //4.双流join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });
        //5.写入kafka
        paymentWideDS.print("PaymentWide>>>>>>>>>>>>>>>>>>");
        paymentWideDS.map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(paymentSinkTopic));
        //6.启动
        env.execute();
    }
}
