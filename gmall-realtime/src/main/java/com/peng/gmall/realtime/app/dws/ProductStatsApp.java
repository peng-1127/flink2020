package com.peng.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.peng.gmall.realtime.app.func.AsyncDimFunction;
import com.peng.gmall.realtime.bean.OrderWide;
import com.peng.gmall.realtime.bean.PaymentWide;
import com.peng.gmall.realtime.bean.ProductStats;
import com.peng.gmall.realtime.common.GmallConstant;
import com.peng.gmall.realtime.utils.ClickHouseUtil;
import com.peng.gmall.realtime.utils.DateTimeUtil;
import com.peng.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author bp
 * @create 2021-07-06 21:22
 */
//形成以商品为准的统计  曝光 点击  购物车  下单 支付  退单  评论数 宽表
public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        /*//检查点CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop102:8020/gmall/flink/checkpoint/ProductStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","atguigu");*/


        //TODO 2.读取kafka7个主题数据创建流
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> paymentWideSource = MyKafkaUtil.getKafkaConsumer(paymentWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> favorInfoSourceSource = MyKafkaUtil.getKafkaConsumer(favorInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtil.getKafkaConsumer(cartInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> refundInfoSource = MyKafkaUtil.getKafkaConsumer(refundInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> commentInfoSource = MyKafkaUtil.getKafkaConsumer(commentInfoSourceTopic, groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSource);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);
        //TODO 3.统一数据格式
        //3.1 pageViewDStream 点击&曝光
        //不需要返回值
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pageViewDStream.process(new ProcessFunction<String, ProductStats>() {
            @Override
            public void processElement(String value, Context ctx, Collector<ProductStats> out) throws Exception {
                //a.转换为JSON对象
                JSONObject jsonObject = JSONObject.parseObject(value);
                //b.判断是否为点击数据,获取页面信息
                //点击:过滤出点击数据 : page_id是good_detail并且item_type是商品id
                    //{"during_time":5289,"item":"3","item_type":"sku_id","last_page_id":"good_list","page_id":"good_detail","source_type":"promotion"}
                Long ts = jsonObject.getLong("ts");
                JSONObject page = jsonObject.getJSONObject("page");
                if ("good_detail".equals(page.getString("page_id")) && "sku_id".equals(page.getString("item_type"))) {
                    //写出数据:点击数
                    out.collect(ProductStats.builder()
                            .sku_id(page.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build());
                }

                //c.判断是否为曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);
                        if ("good_detail".equals(page.getString("page_id")) && "sku_id".equals(page.getString("item_type"))) {
                            //写出数据:曝光数
                            out.collect(ProductStats.builder()
                                    .sku_id(page.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build());
                        }

                    }
                }
            }
        });
        //3.2 favorInfoDStream 收藏
        SingleOutputStreamOperator<ProductStats> ProductStatsWithFavorDS = favorInfoDStream.map(line -> {
            //将数据转换为JSONObject对象
            JSONObject jsonObject = JSONObject.parseObject(line);
            //写出数据:收藏数
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });
        //3.3 orderWideDStream 下单
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderWideDStream.map(line -> {
            //将数据转化为javaBean
            OrderWide orderWide = JSONObject.parseObject(line, OrderWide.class);

            HashSet<Long> orderIdSet = new HashSet<>();
            orderIdSet.add(orderWide.getOrder_id());
            //写出数据:用于统计订单数,下单商品金额,下单商品个数
            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .order_amount(orderWide.getTotal_amount())
                    .orderIdSet(orderIdSet)
                    .order_sku_num(orderWide.getSku_num())
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .build();
        });
        //3.4 paymentWideDStream 支付
        SingleOutputStreamOperator<ProductStats> productStatsWithPaymentDS = paymentWideDStream.map(line -> {
            //将数据转换为JavaBean
            PaymentWide paymentWide = JSONObject.parseObject(line, PaymentWide.class);
            //写出数据:支付金额,用于统计支付订单数
            HashSet<Long> payOrderIdSet = new HashSet<>();
            payOrderIdSet.add(paymentWide.getOrder_id());
            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .payment_amount(paymentWide.getTotal_amount())
                    .paidOrderIdSet(payOrderIdSet)
                    .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                    .build();
        });
        //3.5 cartInfoDStream 购物车
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartInfoDStream.map(line -> {
            //将数据转换为JSONObject
            JSONObject jsonObject = JSONObject.parseObject(line);
            //写出:添加购物车数
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();

        });
        //3.6 refundInfoDStream 退款
        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundInfoDStream.map(line -> {
            //将数据转为JSONObject
            JSONObject jsonObject = JSONObject.parseObject(line);
            //写出数据:用于退款支付订单数,退款金额
            HashSet<Long> refundOrderIdSet = new HashSet<>();
            refundOrderIdSet.add(jsonObject.getLong("order_id"));
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(refundOrderIdSet)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();

        });
        //3.7 commentInfoDStream 评价
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentInfoDStream.map(line -> {
            //将数据转换成JSONObject
            JSONObject jsonObject = JSONObject.parseObject(line);

            //取出评价数据
            String appraise = jsonObject.getString("appraise");
            long good = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                good = 1L;
            }
            //写出数据:评论订单数,好评订单数
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(good)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });


        //TODO 4.合并7个流
        DataStream<ProductStats> unionDS = productStatsWithCommentDS.union(
                productStatsWithClickAndDisplayDS,
                productStatsWithCartDS,
                productStatsWithOrderDS,
                productStatsWithPaymentDS,
                productStatsWithRefundDS,
                ProductStatsWithFavorDS);

        //TODO 5.分组(SKU_ID)
        KeyedStream<ProductStats, Long> keyedStream = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
            @Override
            public long extractTimestamp(ProductStats element, long recordTimestamp) {
                return element.getTs();
            }
        }))
                .keyBy(new KeySelector<ProductStats, Long>() {
                    @Override
                    public Long getKey(ProductStats value) throws Exception {
                        return value.getSku_id();
                    }
                });

        //TODO 6.开窗聚合
        WindowedStream<ProductStats, Long, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        //聚合
        SingleOutputStreamOperator<ProductStats> result = windowedStream.reduce(new ReduceFunction<ProductStats>() {
            @Override
            public ProductStats reduce(ProductStats value1, ProductStats value2) throws Exception {
                //点击&曝光
                value1.setClick_ct(value1.getClick_ct() + value2.getClick_ct());
                //收藏
                value1.setFavor_ct(value1.getFavor_ct() + value2.getFavor_ct());
                //订单
                value1.setOrder_amount(value1.getOrder_amount().add(value2.getOrder_amount()));
                value1.setOrder_sku_num(value1.getOrder_sku_num() + value2.getOrder_sku_num());
                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                value1.setOrder_ct(value1.getOrderIdSet().size() + 0L);
                //支付
                value1.setPayment_amount(value1.getPayment_amount().add(value2.getPayment_amount()));
                value1.getPaidOrderIdSet().addAll(value2.getPaidOrderIdSet());
                value1.setPaid_order_ct(value1.getPaidOrderIdSet().size() + 0L);
                //购物车
                value1.setCart_ct(value1.getCart_ct() + value2.getCart_ct());
                //退款
                value1.setRefund_amount(value1.getRefund_amount().add(value2.getRefund_amount()));
                value1.getRefundOrderIdSet().addAll(value2.getRefundOrderIdSet());
                value1.setRefund_order_ct(value1.getRefundOrderIdSet().size() + 0L);
                //评价
                value1.setComment_ct(value1.getComment_ct() + value2.getComment_ct());
                value1.setGood_comment_ct(value1.getGood_comment_ct() + value2.getGood_comment_ct());
                return value1;
            }
        }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
            @Override
            public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {
                //取出数据
                ProductStats productStats = input.iterator().next();
                //补充窗口的开始和结束字段
                String stt = DateTimeUtil.toYMDhms(new Date(window.getStart()));
                String end = DateTimeUtil.toYMDhms(new Date(window.getEnd()));

                //将数据放进javaBean
                productStats.setStt(stt);
                productStats.setEdt(end);

                //写出数据
                out.collect(productStats);

            }
        });

        //TODO 7.关联商品维度信息
        //7.1 SKU
        SingleOutputStreamOperator<ProductStats> productStatsWithSKUDS = AsyncDataStream.unorderedWait(result, new AsyncDimFunction<ProductStats>("DIM_SKU_INFO") {
            @Override
            public String getKey(ProductStats input) {
                return input.getSku_id().toString();
            }

            @Override
            public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                //获取(get)字段商品价格,商品名称,spu_id,TM_id,Catalog_id
                BigDecimal price = dimInfo.getBigDecimal("PRICE");
                String sku_name = dimInfo.getString("SKU_NAME");

                Long spu_id = dimInfo.getLong("SPU_ID");
                Long tm_id = dimInfo.getLong("TM_ID");
                Long category3_id = dimInfo.getLong("CATEGORY3_ID");

                //补充信息(set)
                input.setSku_price(price);
                input.setSku_name(sku_name);
                input.setSpu_id(spu_id);
                input.setTm_id(tm_id);
                input.setCategory3_id(category3_id);

            }
        }, 60, TimeUnit.SECONDS);

        //7.2 SPU
        SingleOutputStreamOperator<ProductStats> productStatsWithSPUDS = AsyncDataStream.unorderedWait(productStatsWithSKUDS, new AsyncDimFunction<ProductStats>("DIM_SPU_INFO") {
            @Override
            public String getKey(ProductStats input) {
                return input.getSpu_id().toString();
            }

            @Override
            public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                //取出数据
                String name = dimInfo.getString("SPU_NAME");
                //补充数据
                input.setSpu_name(name);
            }
        }, 60, TimeUnit.SECONDS);
        //7.3 TM
        SingleOutputStreamOperator<ProductStats> productStatsWithTMDS = AsyncDataStream.unorderedWait(productStatsWithSPUDS, new AsyncDimFunction<ProductStats>("DIM_BASE_TRADEMARK") {
            @Override
            public String getKey(ProductStats input) {
                return input.getTm_id().toString();
            }

            @Override
            public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                //取出数据
                String tm_name = dimInfo.getString("TM_NAME");
                //补充数据
                input.setTm_name(tm_name);
            }
        }, 60, TimeUnit.SECONDS);
        //7.4 Category
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS = AsyncDataStream.unorderedWait(productStatsWithTMDS, new AsyncDimFunction<ProductStats>("DIM_BASE_CATEGORY3") {
            @Override
            public String getKey(ProductStats input) {
                return input.getCategory3_id().toString();
            }

            @Override
            public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                //取出数据
                String name = dimInfo.getString("NAME");
                //补充数据
                input.setCategory3_name(name);
            }
        }, 60, TimeUnit.SECONDS);
        //TODO 8.将数据写入ClickHouse
        productStatsWithCategory3DS.print("ALL>>>>>>>>>>>>>>>>>>>>>");
        productStatsWithCategory3DS.addSink(ClickHouseUtil.getJdbcSink("insert into product_stats_0108 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 9.启动
        env.execute();


    }
}
