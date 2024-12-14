package com.bw.dwd.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bw.base.BaseApp;
import com.bw.common.Constant;
import com.bw.utils.DateFormatUtil;
import com.bw.utils.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseLog().start(10011,4,Constant.TOPIC_DWD_BASE_LOG, Constant.TOPIC_LOG);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource) {
        //查看数据
//        streamSource.print();
        //数据清洗
        SingleOutputStreamOperator<JSONObject> etlStream = etl(streamSource);
        //添加水位线，解决数据乱序问题，并分组，使同一mid层
        KeyedStream<JSONObject, String> keyByStream = getKeyedStream(etlStream);
        //新老用户校验
        SingleOutputStreamOperator<JSONObject> mapStream = fixIsNew(keyByStream);
        //分流
        OutputTag<String> errTag = new OutputTag<>("err", TypeInformation.of(String.class));
        OutputTag<String> startTag = new OutputTag<>("start", TypeInformation.of(String.class));
        OutputTag<String> displayTag = new OutputTag<>("display", TypeInformation.of(String.class));
        OutputTag<String> actionTag = new OutputTag<>("action", TypeInformation.of(String.class));
        //主流分出测流
        SingleOutputStreamOperator<String> processStream = getSplitStream(mapStream,errTag,startTag,displayTag,actionTag);
        //将数据写入到kafka
        SideOutputDataStream<String> errOut = processStream.getSideOutput(errTag);
        SideOutputDataStream<String> startOut = processStream.getSideOutput(startTag);
        SideOutputDataStream<String> displayOut = processStream.getSideOutput(displayTag);
        SideOutputDataStream<String> actionOut = processStream.getSideOutput(actionTag);

        errOut.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        startOut.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        displayOut.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionOut.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        processStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));

    }

    /**
     * 将数据分流
     * @param mapStream
     * @param errTag
     * @param startTag
     * @param displayTag
     * @param actionTag
     * @return
     */
    public SingleOutputStreamOperator<String> getSplitStream(SingleOutputStreamOperator<JSONObject> mapStream,OutputTag<String> errTag, OutputTag<String> startTag,OutputTag<String> displayTag,OutputTag<String> actionTag){
        return mapStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                //错误日志信息
                JSONObject err = jsonObject.getJSONObject("err");
                //错误日志如果存在
                if (err != null) {
                    //存入错误测流
                    context.output(errTag, jsonObject.toJSONString());
                    //从数据中删除错误日志信息
                    jsonObject.remove("err");
                }
                //获取common
                JSONObject common = jsonObject.getJSONObject("common");
                //获取启动日志信息
                JSONObject start = jsonObject.getJSONObject("start");
                //获取页面日志信息
                JSONObject page = jsonObject.getJSONObject("page");
                //获取时间戳
                Long ts = jsonObject.getLong("ts");
                //判断是否是启动日志
                if (start != null) {
                    //存入错误测流
                    context.output(startTag, jsonObject.toJSONString());
                    //从数据中删除错误日志信息
                    jsonObject.remove("start");
                } else if (page != null) {
                    //当页面日志不为空时将不同的日志信息放在不同的流中
                    //获取曝光日志
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("page", page);
                            display.put("common", common);
                            display.put("ts", ts);
                            context.output(displayTag, display.toJSONString());
                        }
                        jsonObject.remove("displays");
                    }
                    //获取活动日志
                    JSONArray actions = jsonObject.getJSONArray("actions");
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("page", page);
                            action.put("common", common);
                            action.put("ts", ts);
                            context.output(actionTag, action.toJSONString());
                        }
                        jsonObject.remove("actions");
                    }
                    collector.collect(jsonObject.toJSONString());
                }
            }
        });
    }
    /**
     * 新老用户校验
     * @param keyByStream
     * @return
     */
    public SingleOutputStreamOperator<JSONObject> fixIsNew(KeyedStream<JSONObject, String> keyByStream){
        return keyByStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", String.class));
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject common = jsonObject.getJSONObject("common");
                String isNew = common.getString("is_new");
                Long ts = jsonObject.getLong("ts");
                String toDate = DateFormatUtil.tsToDate(ts);
                String value = state.value();
                if ("1".equals(isNew)) {
                    if (null == value) {
                        state.update(toDate);
                    } else if (!toDate.equals(value)) {
                        common.put("is_new", 0);
                    }
                } else if ("0".equals(isNew)) {
                    state.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000));
                }
                return jsonObject;
            }
        });
    }
    /**
     * 添加时间语义并使用mid进行分组
     * @param etlStream
     * @return
     */
    public KeyedStream<JSONObject, String> getKeyedStream(SingleOutputStreamOperator<JSONObject> etlStream){
        return etlStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                })).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });
    }
    /**
     * 数据清洗
     * @param streamSource
     * @return
     */
    public SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> streamSource){
        return streamSource.flatMap(new RichFlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    String ts = jsonObject.getString("ts");
                    JSONObject common = jsonObject.getJSONObject("common");
                    String mid = common.getString("mid");
                    if (!ts.isEmpty() && !mid.isEmpty()) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
