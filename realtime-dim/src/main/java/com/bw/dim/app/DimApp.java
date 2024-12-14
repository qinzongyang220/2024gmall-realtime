package com.bw.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.base.BaseApp;
import com.bw.bean.TableProcessDim;
import com.bw.common.Constant;
import com.bw.dim.functions.DimProcessFunction;
import com.bw.dim.functions.DimSinkFunction;
import com.bw.utils.FlinkSourceUtil;
import com.bw.utils.HbaseUtil;
import com.bw.utils.JdbcUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(10001,4, Constant.DIM_APP,Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource) {
        //测试数据链接
//        streamSource.print();
        // 数据清洗
        SingleOutputStreamOperator<JSONObject> etlStream = etl(streamSource);
//        etlStream.print();
        // CDC读取配置表数据 并行度只能为1
        DataStreamSource<String> cdcStream = env.fromSource(FlinkSourceUtil.getMysqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DIM_TABLE_NAME), WatermarkStrategy.noWatermarks(), "cdc_source").setParallelism(1);
//        cdcStream.print();
        // 在Hbase建表
        SingleOutputStreamOperator<TableProcessDim> createTablesStream = createTable(cdcStream);
        // 新建描述器
        MapStateDescriptor<String, TableProcessDim> mapDescriptor = new MapStateDescriptor<>("mapDescriptor", String.class, TableProcessDim.class);
        // 创建广播流
        BroadcastStream<TableProcessDim> broadcastStream = createTablesStream.broadcast(mapDescriptor);
        //主流链接广播流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> processStream = etlStream.connect(broadcastStream).process(new DimProcessFunction(mapDescriptor));
        // 过滤列
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dataStream = getFilterStream(processStream);
        //写入Hbase
        dataStream.addSink(new DimSinkFunction());
    }
    public SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> getFilterStream(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> processStream){
        return processStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
            /**
             * 过滤出data中不应该存入hbase的列
             * @param jsonObjectTableProcessDimTuple2
             * @return
             * @throws Exception
             */
            @Override
            public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> jsonObjectTableProcessDimTuple2) throws Exception {
                //获取主流数据
                JSONObject f0 = jsonObjectTableProcessDimTuple2.f0;
                // 获取配置流数据
                TableProcessDim f1 = jsonObjectTableProcessDimTuple2.f1;
                //获取配置流中的所有列名
                List<String> strings = Arrays.asList(f1.getSinkColumns().split(","));
                //获取主流中的数据
                JSONObject data = f0.getJSONObject("data");
                //将不需要的列删除
                data.keySet().removeIf(key -> !strings.contains(key));
                //返回处理后的数据
                return jsonObjectTableProcessDimTuple2;
            }
        });
    }
    /**
     * hbase建表
     * @param cdcStream
     * @return
     */
    public SingleOutputStreamOperator<TableProcessDim> createTable(DataStreamSource<String> cdcStream){
        return cdcStream.flatMap(new RichFlatMapFunction<String, TableProcessDim>() {
            private Connection hbaseConnect;
            private TableProcessDim tableProcessDim;

            /**
             * 链接Hbase
             * @param parameters
             * @throws Exception
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseConnect = HbaseUtil.getHbaseConnect();
            }

            /**
             * 关闭Hbase链接
             * @throws Exception
             */
            @Override
            public void close() throws Exception {
                if (hbaseConnect != null) {
                    hbaseConnect.close();
                }
            }

            /**
             * 根据数据状态建表
             * @param s
             * @param collector
             * @throws Exception
             */
            @Override
            public void flatMap(String s, Collector<TableProcessDim> collector) throws Exception {
                //转换为Json格式
                JSONObject jsonObject = JSON.parseObject(s);
                //获取op 本条数据状态
                String op = jsonObject.getString("op");
                // 根据状态对表操作
                if ("d".equals(op)) {
                    tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                    deleteTable();
                } else if ("r".equals(op) || "c".equals(op)) {
                    tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                    String[] split = tableProcessDim.getSinkFamily().split(",");
                    createTable(split);
                } else {
                    tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                    deleteTable();
                    String[] split = tableProcessDim.getSinkFamily().split(",");
                    createTable(split);
                }
            }

            /**
             * 删除表
             */
            public void deleteTable() {
                System.out.println("删除表：" + tableProcessDim.getSinkTable());
                try {
                    HbaseUtil.dropHBaseTable(hbaseConnect, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            /**
             * 创建表
             * @param family
             */
            public void createTable(String[] family) {
                System.out.println("创建表：" + tableProcessDim.getSinkTable());
                try {
                    HbaseUtil.createHBaseTable(hbaseConnect, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), family);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }).setParallelism(1);
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
                    String database = jsonObject.getString("database");
                    String type = jsonObject.getString("type");
                    String data = jsonObject.getString("data");
                    if (!data.isEmpty()) {
                        if ("gmall2024-realtime".equals(database) && !"bootstrap-start".equals(type) && !"bootstrap-complete".equals(type)) {
                            collector.collect(jsonObject);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
