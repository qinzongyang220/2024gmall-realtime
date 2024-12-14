package com.bw.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.base.BaseApp;
import com.bw.bean.CartAddUuBean;
import com.bw.bean.TradePaymentBean;
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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;
import java.util.Iterator;

public class DwsTradePaymentSucWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradePaymentSucWindow().start(10027,4, Constant.DWS_TRADE_PAYMENT_SUC_WINDOW,Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource) {
        //数据清洗 ETL
        SingleOutputStreamOperator<JSONObject> etlStream = getEtlStream(streamSource);
        //分组聚合转换为实体类
        SingleOutputStreamOperator<TradePaymentBean> processStream = getProcessStream(etlStream);
        //添加水位线 开窗聚合
        SingleOutputStreamOperator<TradePaymentBean> reduceStream = getReduceStream(processStream);
        //写入Doris
        reduceStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_PAYMENT_SUC_WINDOW));
    }

    /**
     * 添加水位线 开窗 聚合
     * @param processStream
     * @return
     */
    private static SingleOutputStreamOperator<TradePaymentBean> getReduceStream(SingleOutputStreamOperator<TradePaymentBean> processStream) {
        return processStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradePaymentBean>() {
                    @Override
                    public TradePaymentBean reduce(TradePaymentBean t1, TradePaymentBean t2) throws Exception {
                        t1.setPaymentSucUniqueUserCount(t1.getPaymentSucUniqueUserCount() + t2.getPaymentSucUniqueUserCount());
                        t1.setPaymentSucNewUserCount(t1.getPaymentSucNewUserCount() + t2.getPaymentSucNewUserCount());
                        return t1;
                    }
                }, new AllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<TradePaymentBean> iterable, Collector<TradePaymentBean> collector) throws Exception {
                        String s1 = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String s2 = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String s3 = DateFormatUtil.tsToDate(new Date().getTime());
                        Iterator<TradePaymentBean> iterator = iterable.iterator();
                        while (iterator.hasNext()) {
                            TradePaymentBean next = iterator.next();
                            next.setStt(s1);
                            next.setEdt(s2);
                            next.setCurDate(s3);
                            collector.collect(next);
                        }
                    }
                });
    }

    /**
     * 分组聚合转化为实体类
     * @param etlStream
     * @return
     */
    private SingleOutputStreamOperator<TradePaymentBean> getProcessStream(SingleOutputStreamOperator<JSONObject> etlStream){
        return etlStream.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject JSONObject, long l) {
                                return JSONObject.getLong("ts")*1000;
                            }
                        }).withIdleness(Duration.ofSeconds(5))).keyBy(x -> x.getString("user_id"))
                .process(new KeyedProcessFunction<String, JSONObject, TradePaymentBean>() {
                    private ValueState<String> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> state1 = new ValueStateDescriptor<>("state", String.class);
                        state = getRuntimeContext().getState(state1);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradePaymentBean>.Context context, Collector<TradePaymentBean> collector) throws Exception {
                        Long ts = jsonObject.getLong("ts");
                        ts = ts * 1000;
                        String value = state.value();
                        long puc = 0L;
                        long pnc = 0L;
                        String toDate = DateFormatUtil.tsToDate(ts);
                        if (!toDate.equals(value)) {
                            puc = 1L;
                            state.update(toDate);
                            if (value == null) {
                                pnc = 1L;
                            }
                        }
                        if (puc > 0) {
                            collector.collect(TradePaymentBean.builder().paymentSucNewUserCount(puc).paymentSucUniqueUserCount(pnc).ts(ts).build());
                        }
                    }
                });
    }
    /**
     * 数据清晰 ETL
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
                    String user_id = jsonObject.getString("user_id");
                    if (!user_id.isEmpty()&& ts>0)
                        collector.collect(jsonObject);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
