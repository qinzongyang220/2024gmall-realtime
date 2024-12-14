package com.bw.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.base.BaseApp;
import com.bw.bean.ShopBean;
import com.bw.common.Constant;
import com.bw.functions.Async;
import com.bw.utils.DateFormatUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class DwsShopTop extends BaseApp {
    public static void main(String[] args) {
        new DwsShopTop().start(10032,4, Constant.DWS_SHOP_TOP,Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource) {
        //数据清洗ETL
        SingleOutputStreamOperator<ShopBean> etlStream = getEtlStream(streamSource);
//        etlStream.print();
        //添加水位线开窗聚合
        SingleOutputStreamOperator<ShopBean> reduceStream = getReduceStream(etlStream);
//        reduceStream.print();
        //关联维度
        SingleOutputStreamOperator<ShopBean> shopStream = getShopStream(reduceStream);
        //写到Mysql
//        shopStream.addSink(new MySink());
    }

    /**
     * 关联维度
     * @param reduceStream
     * @return
     */
    private static SingleOutputStreamOperator<ShopBean> getShopStream(SingleOutputStreamOperator<ShopBean> reduceStream) {
        return AsyncDataStream.unorderedWait(reduceStream, new Async<ShopBean>("dim_base_shop") {
            @Override
            public String getDimKey(ShopBean shopBean) {
                return shopBean.getShopId();
            }

            @Override
            public void setTable(ShopBean shopBean, JSONObject jsonObject) {
                shopBean.setShopName(jsonObject.getString("name"));
            }
        }, 100, TimeUnit.SECONDS);
    }

    private static SingleOutputStreamOperator<ShopBean> getReduceStream(SingleOutputStreamOperator<ShopBean> etlStream) {
        return etlStream.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<ShopBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<ShopBean>() {
                            @Override
                            public long extractTimestamp(ShopBean jsonObject, long l) {
                                return jsonObject.getTs();
                            }
                        }).withIdleness(Duration.ofSeconds(10)))
                .keyBy(x -> x.getShopId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ShopBean>() {
                    @Override
                    public ShopBean reduce(ShopBean s1, ShopBean s2) throws Exception {
                        s1.setAmount(s1.getAmount().add(s2.getAmount()));
                        return s1;
                    }
                }, new ProcessWindowFunction<ShopBean, ShopBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<ShopBean, ShopBean, String, TimeWindow>.Context context, Iterable<ShopBean> iterable, Collector<ShopBean> collector) throws Exception {
                        TimeWindow timeWindow = context.window();
                        String s1 = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String s2 = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String s3 = DateFormatUtil.tsToDate(new Date().getTime());
                        Iterator<ShopBean> iterator = iterable.iterator();
                        while (iterator.hasNext()) {
                            ShopBean next = iterator.next();
                            next.setStt(s1);
                            next.setEdt(s2);
                            next.setCurDate(s3);
                            collector.collect(next);
                        }
                    }
                });
    }

    /**
     * 数据清洗ETL
     * @param streamSource
     * @return
     */
    private static SingleOutputStreamOperator<ShopBean> getEtlStream(DataStreamSource<String> streamSource) {
        return streamSource.flatMap(new FlatMapFunction<String, ShopBean>() {
            @Override
            public void flatMap(String s, Collector<ShopBean> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    if (!jsonObject.isEmpty()) {
                        BigDecimal split_total_amount = jsonObject.getBigDecimal("split_total_amount");
                        String create_time = jsonObject.getString("create_time");
                        long ts = DateFormatUtil.dateToTsTwo(create_time);
                        ShopBean shopBean = ShopBean.builder()
                                .shopId((int) (Math.random() * 12 + 1) + "")
                                .amount(split_total_amount)
                                .ts(ts).build();
                        collector.collect(shopBean);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
