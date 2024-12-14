package com.bw.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.base.BaseApp;
import com.bw.bean.CartAddUuBean;
import com.bw.bean.TrafficHomeDetailPageViewBean;
import com.bw.common.Constant;
import com.bw.functions.DorisMapFunction;
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
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;
import java.util.Iterator;

public class DwsTradeCartAddUuWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeCartAddUuWindow().start(10026,4, Constant.DWS_TRADE_CART_ADD_UU_WINDOW,Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource) {
        //数据清洗ETL
        SingleOutputStreamOperator<JSONObject> etlStream = getEtlStream(streamSource);
        //添加水位线 分组聚合
        SingleOutputStreamOperator<CartAddUuBean> processStream = getProcessStream(etlStream);
        //开窗聚合
        SingleOutputStreamOperator<CartAddUuBean> reduceStream = getReduceStream(processStream);
        //写入doris
        reduceStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_CART_ADD_UU_WINDOW));
    }

    /**
     * 开窗聚合
     * @param processStream
     * @return
     */
    private static SingleOutputStreamOperator<CartAddUuBean> getReduceStream(SingleOutputStreamOperator<CartAddUuBean> processStream) {
        return processStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean c1, CartAddUuBean c2) throws Exception {
                        c1.setCartAddUuCt(c1.getCartAddUuCt() + c2.getCartAddUuCt());
                        return c1;
                    }
                }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<CartAddUuBean> iterable, Collector<CartAddUuBean> collector) throws Exception {
                        String s1 = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String s2 = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String s3 = DateFormatUtil.tsToDate(new Date().getTime());
                        Iterator<CartAddUuBean> iterator = iterable.iterator();
                        while (iterator.hasNext()) {
                            CartAddUuBean next = iterator.next();
                            next.setStt(s1);
                            next.setEdt(s2);
                            next.setCurDate(s3);
                            collector.collect(next);
                        }
                    }
                });
    }

    /**
     * 添加水位线 使用用户Id分组聚合，转换为实体类
     * @param etlStream
     * @return
     */
    private SingleOutputStreamOperator<CartAddUuBean> getProcessStream(SingleOutputStreamOperator<JSONObject> etlStream){
        return etlStream
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        }).withIdleness(Duration.ofSeconds(5)))
                .keyBy(x -> x.getString("user_id"))
                .process(new KeyedProcessFunction<String, JSONObject, CartAddUuBean>() {
                    private ValueState<String> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> state1 = new ValueStateDescriptor<>("state", String.class);
                        state1.enableTimeToLive(StateTtlConfig.newBuilder(Time.hours(24))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build());
                        state = getRuntimeContext().getState(state1);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, CartAddUuBean>.Context context, Collector<CartAddUuBean> collector) throws Exception {
                        Long ts = jsonObject.getLong("ts");
                        String toDate = DateFormatUtil.tsToDate(ts);
                        String value = state.value();
                        long cA = 0L;
                        if (!toDate.equals(value)) {
                            cA = 1L;
                            state.update(toDate);
                        }
                        if (cA > 0) {
                            collector.collect(CartAddUuBean.builder().cartAddUuCt(cA).build());
                        }
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
                    String user_id = jsonObject.getString("user_id");
                    if (ts>0 && !user_id.isEmpty()){
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
