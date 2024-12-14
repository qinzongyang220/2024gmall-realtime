package com.bw.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.base.BaseApp;
import com.bw.bean.TrafficHomeDetailPageViewBean;
import com.bw.bean.TrafficPageViewBean;
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
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;
import java.util.Iterator;

public class DwsTrafficHomeDetailPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficHomeDetailPageViewWindow().start(10023,4, Constant.DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW,Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource) {
        //数据清洗ETL
        SingleOutputStreamOperator<JSONObject> etlStream = getEtlStream(streamSource);
        //分组聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> processStream = getProcessStream(etlStream);
        //添加水位线，并开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceStream = getReduceStream(processStream);
        //写到Doris
        reduceStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW));
    }

    private static SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> getReduceStream(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> processStream) {
        return processStream.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<TrafficHomeDetailPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<TrafficHomeDetailPageViewBean>() {
                            @Override
                            public long extractTimestamp(TrafficHomeDetailPageViewBean trafficPageViewBean, long l) {
                                return trafficPageViewBean.getTs();
                            }
                        })).windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                            @Override
                            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean t1, TrafficHomeDetailPageViewBean t2) throws Exception {
                                t1.setHomeUvCt(t1.getHomeUvCt() + t2.getHomeUvCt());
                                t1.setGoodDetailUvCt(t1.getGoodDetailUvCt() + t2.getGoodDetailUvCt());
                                return t1;
                            }
                        },
                        new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow timeWindow, Iterable<TrafficHomeDetailPageViewBean> iterable, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                                String s1 = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                                String s2 = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                                String s3 = DateFormatUtil.tsToDate(new Date().getTime());
                                Iterator<TrafficHomeDetailPageViewBean> iterator = iterable.iterator();
                                while (iterator.hasNext()) {
                                    TrafficHomeDetailPageViewBean next = iterator.next();
                                    next.setStt(s1);
                                    next.setEdt(s2);
                                    next.setCurDate(s3);
                                    collector.collect(next);
                                }
                            }
                        });
    }

    /**
     * 分组聚合转换为实体类
     * @param etlStream
     * @return
     */
    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> getProcessStream(SingleOutputStreamOperator<JSONObject> etlStream){
        return etlStream.keyBy(x -> x.getJSONObject("common").getString("mid"))
                .process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
                    private ValueState<String> homeState;
                    private ValueState<String> goodState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> homeState1 = new ValueStateDescriptor<>("homeState", String.class);
                        homeState1.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.hours(24))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build());
                        homeState = getRuntimeContext().getState(homeState1);
                        ValueStateDescriptor<String> goodState1 = new ValueStateDescriptor<>("goodState", String.class);
                        goodState1.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.hours(24))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build());
                        goodState = getRuntimeContext().getState(goodState1);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context context, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                        JSONObject page = jsonObject.getJSONObject("page");
                        String page_id = page.getString("page_id");
                        Long ts = jsonObject.getLong("ts");
                        String toDate = DateFormatUtil.tsToDate(ts);

                        long homeUv = 0L;
                        if ("home".equals(page_id) && !toDate.equals(homeState)) {
                            homeUv = 1L;
                            homeState.update(toDate);
                        }
                        long goodUv = 0L;
                        if ("good_detail".equals(page_id) && !toDate.equals(goodState)) {
                            goodUv = 1L;
                            homeState.update(toDate);
                        }
                        if (homeUv + goodUv == 1) {
                            collector.collect(new TrafficHomeDetailPageViewBean("", "", "", homeUv, goodUv, ts));
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
                    JSONObject common = jsonObject.getJSONObject("common");
                    JSONObject page = jsonObject.getJSONObject("page");
                    Long ts = jsonObject.getLong("ts");
                    String mid = common.getString("mid");
                    if (ts>0 && !mid.isEmpty()){
                        String page_id = page.getString("page_id");
                        if ("good_detail".equals(page_id) || "home".equals(page_id))
                            collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
