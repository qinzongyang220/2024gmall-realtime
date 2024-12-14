package com.bw;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.base.BaseApp;
import com.bw.bean.UserLoginBean;
import com.bw.bean.UserRegisterBean;
import com.bw.common.Constant;
import com.bw.functions.DorisMapFunction;
import com.bw.utils.DateFormatUtil;
import com.bw.utils.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;

public class DwsUserUserRegisterWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsUserUserRegisterWindow().start(10025,4, Constant.DWS_USER_USER_REGISTER_WINDOW,Constant.TOPIC_DWD_USER_REGISTER);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource) {
        //数据清洗 ETL
        SingleOutputStreamOperator<UserRegisterBean> etlStream = getEtlStream(streamSource);
//        etlStream.print();
        //添加水位线 开窗聚合
        SingleOutputStreamOperator<UserRegisterBean> reduceStream = getReduceStream(etlStream);
//        reduceStream.print();
        //写入Doris
        reduceStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_USER_USER_REGISTER_WINDOW));
    }

    /**
     * 添加水位线并进行开窗聚合 补充实体类
     * @param etlStream
     * @return
     */
    private static SingleOutputStreamOperator<UserRegisterBean> getReduceStream(SingleOutputStreamOperator<UserRegisterBean> etlStream) {
        return etlStream.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
                            @Override
                            public long extractTimestamp(UserRegisterBean trafficPageViewBean, long l) {
                                return DateFormatUtil.dateToTsTwo(trafficPageViewBean.getCurDate());
                            }
                        }).withIdleness(Duration.ofSeconds(5))).windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                    @Override
                    public UserRegisterBean reduce(UserRegisterBean userRegisterBean, UserRegisterBean t1) throws Exception {
                        userRegisterBean.setRegisterCt(userRegisterBean.getRegisterCt() + t1.getRegisterCt());
                        return userRegisterBean;
                    }
                }, new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<UserRegisterBean> iterable, Collector<UserRegisterBean> collector) throws Exception {
                        String s1 = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String s2 = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String s3 = DateFormatUtil.tsToDate(System.currentTimeMillis());
                        Iterator<UserRegisterBean> iterator = iterable.iterator();
                        while (iterator.hasNext()) {
                            UserRegisterBean next = iterator.next();
                            next.setStt(s1);
                            next.setEdt(s2);
                            next.setCurDate(s3);
                            collector.collect(next);
                        }
                    }
                });
    }

    /**
     * 数据清洗 将不为空的数据装换为实体类
     * @param streamSource
     * @return
     */
    private static SingleOutputStreamOperator<UserRegisterBean> getEtlStream(DataStreamSource<String> streamSource) {
        return streamSource.flatMap(new FlatMapFunction<String, UserRegisterBean>() {
            @Override
            public void flatMap(String s, Collector<UserRegisterBean> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    if (!jsonObject.isEmpty()) {
                        String create_time = jsonObject.getString("create_time");
                        collector.collect(UserRegisterBean.builder().registerCt(1L).curDate(create_time).build());
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
