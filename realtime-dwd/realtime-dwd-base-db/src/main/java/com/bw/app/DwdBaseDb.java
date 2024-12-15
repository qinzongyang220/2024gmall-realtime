package com.bw.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.base.BaseApp;
import com.bw.bean.TableProcessDwd;
import com.bw.common.Constant;
import com.bw.functions.DwdProcessFunction;
import com.bw.utils.FlinkSinkUtil;
import com.bw.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Set;

public class DwdBaseDb extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseDb().start(10019,4, Constant.TOPIC_DB,Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource) {
        //数据清洗 etl
        SingleOutputStreamOperator<JSONObject> etlStream = getEtlStream(streamSource);
        //CDC读取配置表
        DataStreamSource<String> dwdStream = env.fromSource(FlinkSourceUtil.getMysqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DWD_TABLE_NAME), WatermarkStrategy.noWatermarks(), "cdc_stream").setParallelism(1);
        //转换为实体类
        SingleOutputStreamOperator<TableProcessDwd> tableDwdStream = getTableProcessDwdSingleOutputStreamOperator(dwdStream);
        //实例化描述器
        MapStateDescriptor<String, TableProcessDwd> mapDescriptor = new MapStateDescriptor<>("mapDescriptor", String.class, TableProcessDwd.class);
        //创建广播流
        BroadcastStream<TableProcessDwd> broadcastStream = tableDwdStream.broadcast(mapDescriptor);
        //主流和广播流链接
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processStream = etlStream.connect(broadcastStream).process(new DwdProcessFunction(mapDescriptor));
        //过滤字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> filterStream = getFilterStream(processStream);
        filterStream.sinkTo(FlinkSinkUtil.getDwdKafkaSink());
//        filterStream.print();
    }
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> getFilterStream(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processBroadStream) {
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> filterStream = processBroadStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDwd>, Tuple2<JSONObject, TableProcessDwd>>() {
            @Override
            public Tuple2<JSONObject, TableProcessDwd> map(Tuple2<JSONObject, TableProcessDwd> jsonObjectTableProcessDwdTuple2) throws Exception {
                JSONObject f0 = jsonObjectTableProcessDwdTuple2.f0;
                TableProcessDwd f1 = jsonObjectTableProcessDwdTuple2.f1;
                JSONObject data = f0.getJSONObject("data");
                String sinkColumns = f1.getSinkColumns();
                Set<String> keys = data.keySet();
                keys.removeIf(key -> !sinkColumns.contains(key));
                return jsonObjectTableProcessDwdTuple2;
            }
        });
        return filterStream;
    }
    /**
     * 转换为实体类
     * @param dwdStream
     * @return
     */
    public SingleOutputStreamOperator<TableProcessDwd> getTableProcessDwdSingleOutputStreamOperator(DataStreamSource<String> dwdStream){
        return dwdStream.flatMap(new FlatMapFunction<String, TableProcessDwd>() {
            @Override
            public void flatMap(String s, Collector<TableProcessDwd> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                TableProcessDwd tableProcessDwd;
                String op = jsonObject.getString("op");
                if ("d".equals(op)) {
                    tableProcessDwd = JSON.parseObject(jsonObject.getString("before"), TableProcessDwd.class);
                } else {
                    tableProcessDwd = JSON.parseObject(jsonObject.getString("after"), TableProcessDwd.class);
                }
                tableProcessDwd.setOp(op);
                collector.collect(tableProcessDwd);
            }
        });
    }
    /**
     * etl
     * @param streamSource
     * @return
     */
    private static SingleOutputStreamOperator<JSONObject> getEtlStream(DataStreamSource<String> streamSource) {
        return streamSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    JSONObject data = jsonObject.getJSONObject("data");
                    if (!data.isEmpty()) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
