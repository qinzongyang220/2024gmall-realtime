package com.bw.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.base.BaseApp;
import com.bw.bean.TradeProvinceOrderBean;
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
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
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
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class DwsTradeProvinceOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindow().start(10030,4, Constant.DWS_TRADE_PROVINCE_ORDER_WINDOW,Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource) {
        //数据清洗 ETL
        SingleOutputStreamOperator<JSONObject> etlStream = getEtlStream(streamSource);
        //添加水位线 分组聚合 以防回撤流重复计算
        SingleOutputStreamOperator<TradeProvinceOrderBean> processStream = getProcessStream(etlStream);
        //分组 开窗聚合
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceStream = getReduceStream(processStream);
        //关联维度
        SingleOutputStreamOperator<TradeProvinceOrderBean> mapStream = reduceStream.map(new Synchronous());
        //写入Doris
        mapStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_PROVINCE_ORDER_WINDOW));
    }

    /**
     * 根据省份ID分组
     * 开窗聚合
     * @param processStream
     * @return
     */
    private static SingleOutputStreamOperator<TradeProvinceOrderBean> getReduceStream(SingleOutputStreamOperator<TradeProvinceOrderBean> processStream) {
        return processStream.keyBy(x -> x.getProvinceId())
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)))
                .reduce(new ReduceFunction<TradeProvinceOrderBean>() {
                            @Override
                            public TradeProvinceOrderBean reduce(TradeProvinceOrderBean t1, TradeProvinceOrderBean t2) throws Exception {
                                t1.setOrderAmount(t1.getOrderAmount().add(t2.getOrderAmount()));
                                t1.getOrderIdSet().addAll(t2.getOrderIdSet());
                                return t1;
                            }
                        },
                        new ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>.Context context, Iterable<TradeProvinceOrderBean> iterable, Collector<TradeProvinceOrderBean> collector) throws Exception {
                                TimeWindow timeWindow = context.window();
                                String s1 = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                                String s2 = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                                String s3 = DateFormatUtil.tsToDate(new Date().getTime());
                                Iterator<TradeProvinceOrderBean> iterator = iterable.iterator();
                                while (iterator.hasNext()) {
                                    TradeProvinceOrderBean next = iterator.next();
                                    next.setStt(s1);
                                    next.setEdt(s2);
                                    next.setCurDate(s3);
                                    int size = next.getOrderIdSet().size();
                                    next.setOrderCount((long) size);
                                    collector.collect(next);
                                }
                            }
                        });
    }

    /**
     * 将数据添加水位线
     * 根据Id分组聚合
     * 解决回撤流数据问题
     * @param etlStream
     * @return
     */
    private SingleOutputStreamOperator<TradeProvinceOrderBean> getProcessStream(SingleOutputStreamOperator<JSONObject> etlStream){
        return etlStream.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        }).withIdleness(Duration.ofSeconds(5)))
                .keyBy(x -> x.getString("id"))
                .process(new KeyedProcessFunction<String, JSONObject, TradeProvinceOrderBean>() {
                    private ValueState<BigDecimal> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<BigDecimal> state1 = new ValueStateDescriptor<>("state", BigDecimal.class);
                        state1.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build());
                        state = getRuntimeContext().getState(state1);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradeProvinceOrderBean>.Context context, Collector<TradeProvinceOrderBean> collector) throws Exception {
                        //获取数据中各种值
                        BigDecimal split_total_amount = jsonObject.getBigDecimal("split_total_amount");
                        Long ts = jsonObject.getLong("ts");
                        String province_id = jsonObject.getString("province_id");
                        String id = jsonObject.getString("id");
                        String order_id = jsonObject.getString("order_id");
                        //将每条数据的订单ID存带set中
                        Set<String> set = new HashSet<>();
                        set.add(order_id);
                        //从状态中获取值
                        BigDecimal sta = state.value() == null ? new BigDecimal(0) : state.value();
                        //将数据转换为实体类并传向下游
                        collector.collect(TradeProvinceOrderBean.builder()
                                .provinceId(province_id)
                                .orderAmount(split_total_amount.subtract(sta))
                                .ts(ts)
                                .orderDetailId(id)
                                .orderIdSet(set)
                                .build());
                        //更新状态中的数据
                        state.update(split_total_amount);
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
                    String province_id = jsonObject.getString("province_id");
                    if (ts > 0 && !province_id.isEmpty()) {
                        jsonObject.put("ts", ts * 1000);
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
