package com.bw.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.base.BaseApp;
import com.bw.bean.TrafficHomeDetailPageViewBean;
import com.bw.bean.UserLoginBean;
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

public class DwsUserUserLoginWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsUserUserLoginWindow().start(10024,4, Constant.DWS_USER_USER_LOGIN_WINDOW,Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource) {
        //数据清洗 etl
        SingleOutputStreamOperator<JSONObject> etlStream = getEtlStream(streamSource);
        //分组聚合转换为实体类
        SingleOutputStreamOperator<UserLoginBean> processStream = getProcessStream(etlStream);
        //添加水位线 开窗聚合
        SingleOutputStreamOperator<UserLoginBean> reduceStream = getReduceStream(processStream);
        //写入Doris
        reduceStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_USER_USER_LOGIN_WINDOW));
    }

    /**
     * 添加水位线，解决乱序
     * 开窗聚合并填充实体类
     * @param processStream
     * @return
     */
    private static SingleOutputStreamOperator<UserLoginBean> getReduceStream(SingleOutputStreamOperator<UserLoginBean> processStream) {
        return processStream.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<UserLoginBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserLoginBean>() {
                            @Override
                            public long extractTimestamp(UserLoginBean trafficPageViewBean, long l) {
                                return trafficPageViewBean.getTs();
                            }
                        })).windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean u1, UserLoginBean u2) throws Exception {
                        u1.setBackCt(u1.getBackCt() + u2.getBackCt());
                        u1.setUuCt(u1.getUuCt() + u2.getUuCt());
                        return u1;
                    }
                }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<UserLoginBean> iterable, Collector<UserLoginBean> collector) throws Exception {
                        String s1 = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String s2 = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String s3 = DateFormatUtil.tsToDate(System.currentTimeMillis());
                        Iterator<UserLoginBean> iterator = iterable.iterator();
                        while (iterator.hasNext()) {
                            UserLoginBean next = iterator.next();
                            next.setStt(s1);
                            next.setEdt(s2);
                            next.setCurDate(s3);
                            collector.collect(next);
                        }
                    }
                });
    }

    /**
     * 根据用户分组聚合转换为实体类
     * @param etlStream
     * @return
     */
    private SingleOutputStreamOperator<UserLoginBean> getProcessStream(SingleOutputStreamOperator<JSONObject> etlStream){
        return etlStream.keyBy(x -> x.getJSONObject("common").getString("uid"))
                .process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
                    private ValueState<String> lastTime;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> lastTime1 = new ValueStateDescriptor<>("lastTime", String.class);
                        lastTime = getRuntimeContext().getState(lastTime1);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context context, Collector<UserLoginBean> collector) throws Exception {
                        Long ts = jsonObject.getLong("ts");
                        String toDate = DateFormatUtil.tsToDate(ts);
                        long baC = 0L;
                        long uuC = 0L;
                        String value = lastTime.value();
                        if (!toDate.equals(value)) {
                            uuC = 1L;
                            lastTime.update(toDate);
                            if (value != null) {
                                long l = DateFormatUtil.dateToTs(value);
                                if ((ts - l) / 1000 / 60 / 60 / 24 > 7) {
                                    baC = 1L;
                                }
                            }
                        }
                        if (uuC == 1) {
                            collector.collect(new UserLoginBean("", "", "", baC, uuC, ts));
                        }
                    }
                });
    }
    /**
     * 数据清洗 ETL
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
                    String uid = common.getString("uid");
                    String last_page_id = page.getString("last_page_id");
                    if (ts > 0 && uid != null && (last_page_id == null || "login".equals(last_page_id))) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
