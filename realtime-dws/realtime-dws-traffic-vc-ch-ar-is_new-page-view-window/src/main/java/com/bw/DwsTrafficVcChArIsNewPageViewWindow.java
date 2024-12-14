package com.bw;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.base.BaseApp;
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
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;
import java.util.Iterator;

public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindow().start(10022,4, Constant.DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW,Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource) {
        //数据清洗
        SingleOutputStreamOperator<JSONObject> etlStream = getEtlStream(streamSource);
        //将数据转换为实体类 并添加水位线
        SingleOutputStreamOperator<TrafficPageViewBean> processStream = getProcessStream(etlStream);
        //根据维度分组聚合
        SingleOutputStreamOperator<TrafficPageViewBean> reduceStream = getReduceStream(processStream);
        //写入Doris
        reduceStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW));
    }

    /**
     * 根据维度开窗，并 补 全对象
     * @param processStream
     * @return
     */
    private static SingleOutputStreamOperator<TrafficPageViewBean> getReduceStream(SingleOutputStreamOperator<TrafficPageViewBean> processStream) {
        return processStream.keyBy(x -> (x.getVc() + "-" + x.getCh() + "-" + x.getAr() + "-" + x.getIsNew()))
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TrafficPageViewBean>() {
                            @Override
                            public TrafficPageViewBean reduce(TrafficPageViewBean v1, TrafficPageViewBean v2) throws Exception {
                                v1.setUvCt(v1.getUvCt() + v2.getUvCt());
                                v1.setSvCt(v1.getSvCt() + v2.getSvCt());
                                v1.setPvCt(v1.getPvCt() + v2.getPvCt());
                                v1.setDurSum(v1.getDurSum() + v2.getDurSum());
                                return v1;
                            }
                        },
                        new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow timeWindow, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) throws Exception {
                                String s1 = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                                String s2 = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                                String s3 = DateFormatUtil.tsToDate(new Date().getTime());
                                Iterator<TrafficPageViewBean> iterator = iterable.iterator();
                                while (iterator.hasNext()) {
                                    TrafficPageViewBean next = iterator.next();
                                    next.setStt(s1);
                                    next.setEdt(s2);
                                    next.setCur_date(s3);
                                    collector.collect(next);
                                }
                            }
                        });
    }
    /**
     * 根据mid分组转换为实体类
     * 再对实体类中的sid分组，求出svct的值
     * 并添加水位线
     * @param etlStream
     * @return
     */
    private SingleOutputStreamOperator<TrafficPageViewBean> getProcessStream(SingleOutputStreamOperator<JSONObject> etlStream){
        return etlStream.keyBy(j -> j.getJSONObject("common").getString("mid"))
                .process(new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {
                    private ValueState<String> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("myState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig
                                .newBuilder(org.apache.flink.api.common.time.Time.hours(24))//状态存储24小时
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build());//当数据发生变化时装载存储时间更新
                        state = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>.Context context, Collector<TrafficPageViewBean> collector) throws Exception {
                        JSONObject common = jsonObject.getJSONObject("common");
                        JSONObject page = jsonObject.getJSONObject("page");
                        //获取时间戳
                        Long ts = jsonObject.getLong("ts");
                        //转换时间格式
                        String toDate = DateFormatUtil.tsToDate(ts);
                        String value = state.value();
                        long uv = 0L;
                        Long pv = 1L;
                        if (!toDate.equals(value)) {
                            uv = 1L;
                            state.update(toDate);
                        }
                        TrafficPageViewBean tb = new TrafficPageViewBean();
                        tb.setVc(common.getString("vc"));
                        tb.setCh(common.getString("ch"));
                        tb.setAr(common.getString("ar"));
                        tb.setIsNew(common.getString("is_new"));
                        tb.setUvCt(uv);
                        tb.setPvCt(pv);
                        tb.setDurSum(page.getLong("during_time"));
                        tb.setTs(ts);
                        tb.setSid(common.getString("sid"));
                        collector.collect(tb);
                    }
                }).keyBy(TrafficPageViewBean::getSid).process(new KeyedProcessFunction<String, TrafficPageViewBean, TrafficPageViewBean>() {
                    private ValueState<String> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", String.class));
                    }

                    @Override
                    public void processElement(TrafficPageViewBean trafficPageViewBean, KeyedProcessFunction<String, TrafficPageViewBean, TrafficPageViewBean>.Context context, Collector<TrafficPageViewBean> collector) throws Exception {
                        String value = state.value();
                        if (value == null) {
                            trafficPageViewBean.setSvCt(1L);
                            state.update(trafficPageViewBean.getSid());
                        } else {
                            trafficPageViewBean.setSvCt(0L);
                        }
                        collector.collect(trafficPageViewBean);
                    }
                }).assignTimestampsAndWatermarks(WatermarkStrategy
                        .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                            @Override
                            public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {
                                return trafficPageViewBean.getTs();
                            }
                        }));
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
                    String mid = common.getString("mid");
                    Long ts = jsonObject.getLong("ts");
                    if (!mid.isEmpty() && ts > 0) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
