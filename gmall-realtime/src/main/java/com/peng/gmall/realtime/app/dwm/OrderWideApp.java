package com.peng.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.peng.gmall.realtime.app.func.AsyncDimFunction;
import com.peng.gmall.realtime.bean.OrderDetail;
import com.peng.gmall.realtime.bean.OrderInfo;
import com.peng.gmall.realtime.bean.OrderWide;
import com.peng.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * @author bp
 * @create 2021-06-29 19:55
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //1.获取流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //设置CK和状态后端
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(5000L);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-210108/cdc/ck"));

        //2.读取kafka 订单与订单详情主题数据并转换为javabean
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        //订单orderInfo
        SingleOutputStreamOperator<OrderInfo> orderInfoKafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderInfoSourceTopic, groupId))
                .map(data -> {
                    OrderInfo orderInfo = JSONObject.parseObject(data, OrderInfo.class);

                    //2020-12-21 17:45:24 补充OrderInfo的javabean中的字段
                    String create_time = orderInfo.getCreate_time();
                    String[] dateTimeArr = create_time.split(" ");
                    orderInfo.setCreate_date(dateTimeArr[0]);   //补充Create_date日期字段
                    orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]); //补充Create_hour小时字段

                    //补充create_ts字段
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderInfo.setCreate_ts(sdf.parse(create_time).getTime());  //把一个字符串变成日期

                    //返回数据
                    return orderInfo;
                });


        //订单明细orderDetail
        SingleOutputStreamOperator<OrderDetail> orderDetailKafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderDetailSourceTopic, groupId))
                .map(data -> {
                    OrderDetail orderDetail = JSONObject.parseObject(data, OrderDetail.class);

                    //补充Create_ts字段
                    String create_time = orderDetail.getCreate_time();
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderDetail.setCreate_ts(sdf.parse(create_time).getTime());
                    //返回数据
                    return orderDetail;
                });

        //3.提取时间戳watermark
        //订单
        SingleOutputStreamOperator<OrderInfo> orderDS = orderInfoKafkaDS.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
            @Override
            public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                return element.getCreate_ts();
            }
        }));

        //订单详情
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailKafkaDS.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
            @Override
            public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                return element.getCreate_ts();
            }
        }));

        //4.双流join(先做keyBy)
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))  //状态保留多久,生产环境中应该设置最大网络延迟
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        orderWideDS.print("orderWideDS>>>>>>>>>>>>>");
        //5.关联维度
        /*//5.1 关联用户维度
        orderWideDS.map(new MapFunction<OrderWide, OrderWide>() {
            @Override
            public OrderWide map(OrderWide value) throws Exception {
               //根据用户ID查询用户信息
                //将用户信息添加到OrderWide并返回
                return null;
            }
        });*/

        //参数:
        //orderWideDS:传进来的流
        //orderWide中补充字段
        //超时时间:要大于zk和pheonix的时间
        //时间类型

        //1.关联用户表维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
                orderWideDS,
                new AsyncDimFunction<OrderWide>("DIM_USER_INFO") {
                    //获取id
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    //将数据合并,不需要返回值
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        //补充用户性别,从pheonix查询出来的
                        String gender = dimInfo.getString("GENDER");
                        orderWide.setUser_gender(gender);
                        //补充用户年龄,因为是生日,所以转换成年龄
                        String birthday = dimInfo.getString("BIRTHDAY");
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                        //出生的时间戳
                        long ts = simpleDateFormat.parse(birthday).getTime();
                        Long age = (System.currentTimeMillis() - ts) / (1000 * 60 * 60 * 24 * 365L);
                        orderWide.setUser_age(new Integer(age.toString()));

                    }
                },
                60,
                TimeUnit.SECONDS);
//        //用户信息表测试打印
//        orderWideWithUserDS.print("User>>>>>>>>>>>");

        //2.关联省市维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS, new AsyncDimFunction<OrderWide>("DIM_BASE_PROVINCE") {
            //获取省市ID
            @Override
            public String getKey(OrderWide input) {
                return input.getProvince_id().toString();
            }

            //关联省市其他维度字段
            @Override
            public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
                input.setProvince_name(dimInfo.getString("NAME"));
                input.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                input.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                input.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
            }
        }, 60, TimeUnit.SECONDS);

        //测试
//        orderWideWithProvinceDS.print("Province>>>>>>>>>>>>>>>>>>>");

        //3.关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(orderWideWithProvinceDS, new AsyncDimFunction<OrderWide>("DIM_SKU_INFO") {
            //获取SKU ID
            @Override
            public String getKey(OrderWide input) {
                return input.getSku_id().toString();
            }

            //获取SKU字段
            @Override
            public void join(OrderWide input, JSONObject dimInfo) {
                input.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                input.setSpu_id(dimInfo.getLong("SPU_ID"));
                input.setTm_id(dimInfo.getLong("TM_ID"));
            }
        }, 60, TimeUnit.SECONDS);

        //4.关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(orderWideWithSkuDS, new AsyncDimFunction<OrderWide>("DIM_SPU_INFO") {
            //获取SPU ID
            @Override
            public String getKey(OrderWide input) {
                return input.getSpu_id().toString();
            }

            //获取SPU字段
            @Override
            public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
                input.setSpu_name(dimInfo.getString("SPU_NAME"));
            }
        }, 60, TimeUnit.SECONDS);

        //5.关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(orderWideWithSpuDS, new AsyncDimFunction<OrderWide>("DIM_BASE_TRADEMARK") {
            //获取品牌id
            @Override
            public String getKey(OrderWide input) {
                return input.getTm_id().toString();
            }

            //获取品牌名称
            @Override
            public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
                input.setTm_name(dimInfo.getString("TM_NAME"));
            }
        }, 60, TimeUnit.SECONDS);

        //6.关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(orderWideWithTmDS, new AsyncDimFunction<OrderWide>("DIM_BASE_CATEGORY3") {
            //获取品类id
            @Override
            public String getKey(OrderWide input) {
                return input.getCategory3_id().toString();
            }

            //获取品名
            @Override
            public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
                input.setCategory3_name(dimInfo.getString("NAME"));
            }
        }, 60, TimeUnit.SECONDS);


        orderWideWithCategory3DS.print("ALL>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        //6.将数据写入kafka
        orderWideWithCategory3DS.map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(orderWideSinkTopic));
        //7.启动
        env.execute();
    }
}
