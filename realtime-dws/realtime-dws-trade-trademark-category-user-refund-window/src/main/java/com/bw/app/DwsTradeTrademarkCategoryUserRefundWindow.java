package com.bw.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.base.BaseApp;
import com.bw.bean.TradeSkuOrderBean;
import com.bw.bean.TradeTrademarkCategoryUserRefundBean;
import com.bw.common.Constant;
import com.bw.functions.DorisMapFunction;
import com.bw.functions.Synchronous;
import com.bw.functions.SynchronousCache;
import com.bw.utils.DateFormatUtil;
import com.bw.utils.FlinkSinkUtil;
import com.bw.utils.HbaseUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class DwsTradeTrademarkCategoryUserRefundWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeTrademarkCategoryUserRefundWindow().start(10031,4,Constant.DWS_TRADE_TRADEMARK_CATEGORY_USER_REFUND_WINDOW, Constant.TOPIC_DWD_TRADE_ORDER_REFUND);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource) {
        //数据清洗ETL
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> etlStream = getETLStream(streamSource);
        //关联维度表
        //同步
//        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> mapStream = etlStream.map(new Synchronous());
        //同步带缓存
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> mapStream = etlStream.map(new SynchronousCache());
        mapStream.print();
        //添加水位线 分组 开窗 聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceStream = getReduceStream(mapStream);
        reduceStream.print();
        //写入Doris
//        reduceStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_TRADEMARK_CATEGORY_USER_REFUND_WINDOW));
    }

    /**
     * 添加水位线
     * 分组
     * 开窗
     * 聚合
     * @param mapStream
     * @return
     */
    private static SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> getReduceStream(SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> mapStream) {
        return mapStream.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
                            @Override
                            public long extractTimestamp(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, long l) {
                                return tradeTrademarkCategoryUserRefundBean.getTs();
                            }
                        }).withIdleness(Duration.ofSeconds(5)))
                .keyBy(x -> (x.getTrademarkId() + "-" + x.getCategory3Id() + "-" + x.getUserId()))
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean t1, TradeTrademarkCategoryUserRefundBean t2) throws Exception {
                        t1.getOrderIdSet().addAll(t2.getOrderIdSet());
                        return t1;
                    }
                }, new ProcessWindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>.Context context, Iterable<TradeTrademarkCategoryUserRefundBean> iterable, Collector<TradeTrademarkCategoryUserRefundBean> collector) throws Exception {
                        TimeWindow timeWindow = context.window();
                        String s1 = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String s2 = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String s3 = DateFormatUtil.tsToDate(new Date().getTime());
                        Iterator<TradeTrademarkCategoryUserRefundBean> iterator = iterable.iterator();
                        while (iterator.hasNext()) {
                            TradeTrademarkCategoryUserRefundBean next = iterator.next();
                            next.setStt(s1);
                            next.setEdt(s2);
                            next.setCurDate(s3);
                            next.setRefundCount((long) next.getOrderIdSet().size());
                            collector.collect(next);
                        }
                    }
                });
    }

    /**
     * 数据清洗并转换为实体类
     * @param streamSource
     * @return
     */
    private SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> getETLStream(DataStreamSource<String> streamSource){
        return streamSource.flatMap(new FlatMapFunction<String, TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public void flatMap(String s, Collector<TradeTrademarkCategoryUserRefundBean> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    Long ts = jsonObject.getLong("ts");
                    if (ts > 0) {
                        ts = ts * 1000;
                        String user_id = jsonObject.getString("user_id");
                        String sku_id = jsonObject.getString("sku_id");
                        String order_id = jsonObject.getString("order_id");
                        Set<String> set = new HashSet<>();
                        set.add(order_id);
                        collector.collect(TradeTrademarkCategoryUserRefundBean.builder()
                                .ts(ts)
                                .userId(user_id)
                                .skuId(sku_id)
                                .orderIdSet(set)
                                .build());
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
