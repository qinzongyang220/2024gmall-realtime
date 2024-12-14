package com.bw.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.base.BaseApp;
import com.bw.bean.TradeOrderBean;
import com.bw.bean.TradePaymentBean;
import com.bw.common.Constant;
import com.bw.functions.DorisMapFunction;
import com.bw.utils.DateFormatUtil;
import com.bw.utils.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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

public class DwsTradeOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeOrderWindow().start(10028,4, Constant.DWS_TRADE_ORDER_WINDOW,Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource) {
        // 数据清洗ETL
        SingleOutputStreamOperator<JSONObject> etlStream = getEtlStream(streamSource);
        //分组聚合转换为实体类
        SingleOutputStreamOperator<TradeOrderBean> processStream = getProcessStream(etlStream);
        //添加水位线 开窗 聚合
        SingleOutputStreamOperator<TradeOrderBean> reduceStream = getReduceStream(processStream);
        //写入Doris
        reduceStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_ORDER_WINDOW));
    }
    /**
     * 添加水位线 开窗 聚合
     * @param processStream
     * @return
     */
    private static SingleOutputStreamOperator<TradeOrderBean> getReduceStream(SingleOutputStreamOperator<TradeOrderBean> processStream) {
        return processStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradeOrderBean>() {
                    @Override
                    public TradeOrderBean reduce(TradeOrderBean t1, TradeOrderBean t2) throws Exception {
                        t1.setOrderUniqueUserCount(t1.getOrderUniqueUserCount() + t2.getOrderUniqueUserCount());
                        t1.setOrderNewUserCount(t1.getOrderNewUserCount() + t2.getOrderNewUserCount());
                        return t1;
                    }
                }, new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<TradeOrderBean> iterable, Collector<TradeOrderBean> collector) throws Exception {
                        String s1 = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String s2 = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String s3 = DateFormatUtil.tsToDate(new Date().getTime());
                        Iterator<TradeOrderBean> iterator = iterable.iterator();
                        while (iterator.hasNext()) {
                            TradeOrderBean next = iterator.next();
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
    private SingleOutputStreamOperator<TradeOrderBean> getProcessStream(SingleOutputStreamOperator<JSONObject> etlStream){
        return etlStream.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject JSONObject, long l) {
                                return JSONObject.getLong("ts")*1000;
                            }
                        }).withIdleness(Duration.ofSeconds(5)))
                .keyBy(x -> x.getString("user_id"))
                .process(new KeyedProcessFunction<String, JSONObject, TradeOrderBean>() {
                    private ValueState<String> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> state1 = new ValueStateDescriptor<>("state", String.class);
                        state = getRuntimeContext().getState(state1);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradeOrderBean>.Context context, Collector<TradeOrderBean> collector) throws Exception {
                        Long ts = jsonObject.getLong("ts");
                        ts = ts * 1000;
                        String value = state.value();
                        long ouc = 0L;
                        long onc = 0L;
                        String toDate = DateFormatUtil.tsToDate(ts);
                        if (!toDate.equals(value)) {
                            ouc = 1L;
                            state.update(toDate);
                            if (value == null) {
                                onc = 1L;
                            }
                        }
                        if (ouc > 0) {
                            collector.collect(TradeOrderBean.builder().orderUniqueUserCount(ouc).orderNewUserCount(onc).ts(ts).build());
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
