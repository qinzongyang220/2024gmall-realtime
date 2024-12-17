package com.bw.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.base.BaseApp;
import com.bw.bean.CartAddUuBean;
import com.bw.bean.TradeSkuOrderBean;
import com.bw.common.Constant;
import com.bw.functions.DorisMapFunction;
import com.bw.functions.Synchronous;
import com.bw.utils.DateFormatUtil;
import com.bw.utils.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.util.Date;
import java.util.Iterator;

public class DwsTradeSkuOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeSkuOrderWindow().start(10029,4, Constant.DWS_TRADE_SKU_ORDER_WINDOW,Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource) {
        //数据清洗 ETL
        SingleOutputStreamOperator<JSONObject> etlStream = getEtlStream(streamSource);
        //添加水位线 分组 防治回撤流导致重复计算
        SingleOutputStreamOperator<TradeSkuOrderBean> processStream = getProcessStream(etlStream);
        //根据商品分组开窗聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceStream = getReduceStream(processStream);
        //关联维度
        SingleOutputStreamOperator<TradeSkuOrderBean> mapStream = reduceStream.map(new Synchronous());
        //写到Doris中
        mapStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_SKU_ORDER_WINDOW));
//        mapStream.print();
    }

    /**
     * 根据商品Id分组开窗聚合
     * @param processStream
     * @return
     */
    private static SingleOutputStreamOperator<TradeSkuOrderBean> getReduceStream(SingleOutputStreamOperator<TradeSkuOrderBean> processStream) {
        return processStream.keyBy(x -> x.getSkuId())
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)))
                .reduce(new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean t1, TradeSkuOrderBean t2) throws Exception {
                        t1.setOriginalAmount(t1.getOriginalAmount().add(t2.getOriginalAmount()));
                        t1.setCouponReduceAmount(t1.getCouponReduceAmount().add(t2.getCouponReduceAmount()));
                        t1.setActivityReduceAmount(t1.getActivityReduceAmount().add(t2.getActivityReduceAmount()));
                        t1.setOrderAmount(t1.getOrderAmount().add(t2.getOrderAmount()));
                        return t1;
                    }
                }, new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> iterable, Collector<TradeSkuOrderBean> collector) throws Exception {
                        TimeWindow timeWindow = context.window();
                        String s1 = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String s2 = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String s3 = DateFormatUtil.tsToDate(new Date().getTime());
                        Iterator<TradeSkuOrderBean> iterator = iterable.iterator();
                        while (iterator.hasNext()) {
                            TradeSkuOrderBean next = iterator.next();
                            next.setStt(s1);
                            next.setEdt(s2);
                            next.setCurDate(s3);
                            collector.collect(next);
                        }
                    }
                });
    }
    /**
     * 添加水位线
     * 分组聚合 防治回撤流导致数据重复计算
     * @param etlStream
     * @return
     */
    private static SingleOutputStreamOperator<TradeSkuOrderBean> getProcessStream(SingleOutputStreamOperator<JSONObject> etlStream) {
        return etlStream.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        }).withIdleness(Duration.ofSeconds(5)))
                .keyBy(x -> x.getString("id")).process(new KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>() {
                    private MapState<String,BigDecimal> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String,BigDecimal> state1 = new MapStateDescriptor<>("state", String.class,BigDecimal.class);
                        state1.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build());
                        state = getRuntimeContext().getMapState(state1);
                    }
                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>.Context context, Collector<TradeSkuOrderBean> collector) throws Exception {
                        //定义常了量
                        String oa = "original_amount";
                        String saa = "split_activity_amount";
                        String sta = "split_total_amount";
                        String sca = "split_coupon_amount";
                        //获取传来的数据中的值
                        BigDecimal split_activity_amount = jsonObject.getBigDecimal(saa);//活动
                        BigDecimal split_total_amount = jsonObject.getBigDecimal(sta);//分摊
                        BigDecimal split_coupon_amount = jsonObject.getBigDecimal(sca);//优惠
                        BigDecimal order_price = jsonObject.getBigDecimal("order_price");
                        BigDecimal sku_num = jsonObject.getBigDecimal("sku_num");
                        BigDecimal original_amount = order_price.multiply(sku_num);//原始
                        //从状态中获取值
                        BigDecimal saaBD = state.get(saa)== null ? new BigDecimal(0) : state.get(saa) ;
                        BigDecimal staBD = state.get(sta)== null ? new BigDecimal(0) : state.get(sta) ;
                        BigDecimal scaBD = state.get(sca)== null ? new BigDecimal(0) : state.get(sca) ;
                        BigDecimal oaBD = state.get(oa)== null ? new BigDecimal(0) : state.get(oa) ;
                        //获取填充的其他数据
                        String id = jsonObject.getString("id");
                        String skuId = jsonObject.getString("sku_id");
                        String skuName = jsonObject.getString("sku_name");
                        Long ts = jsonObject.getLong("ts");
                        //向下游传输数据
                        collector.collect(TradeSkuOrderBean.builder()
                                        .originalAmount(original_amount.subtract(oaBD))
                                        .orderAmount(split_total_amount.subtract(staBD))
                                        .activityReduceAmount(split_activity_amount.subtract(saaBD))
                                        .couponReduceAmount(split_coupon_amount.subtract(scaBD))
                                        .skuName(skuName)
                                        .skuId(skuId)
                                        .orderDetailId(id)
                                        .ts(ts)
                                .build());
                        //讲数据更新到状态中
                        state.put(saa,split_activity_amount);
                        state.put(sta,split_total_amount);
                        state.put(sca,split_coupon_amount);
                        state.put(oa,original_amount);
                    }
                });
    }

    /**
     * 数据清洗ETL
     * @param streamSource
     * @return
     */
    private static SingleOutputStreamOperator<JSONObject> getEtlStream(DataStreamSource<String> streamSource) {
        return streamSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    Long ts = jsonObject.getLong("ts");
                    jsonObject.put("ts",ts*1000);
                    String skuId = jsonObject.getString("sku_id");
                    if (ts>0 && !skuId.isEmpty()){
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
