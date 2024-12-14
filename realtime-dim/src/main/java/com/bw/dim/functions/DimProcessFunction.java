package com.bw.dim.functions;

import com.alibaba.fastjson.JSONObject;
import com.bw.bean.TableProcessDim;
import com.bw.utils.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;

public class DimProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>> {

    private final HashMap<String,TableProcessDim> hashMap = new HashMap<>();
    private final MapStateDescriptor<String, TableProcessDim> mapDescriptor;

    public DimProcessFunction(MapStateDescriptor<String, TableProcessDim> mapDescriptor) {
        this.mapDescriptor = mapDescriptor;
    }

    //初始化链接存入HashMap
    @Override
    public void open(Configuration parameters) throws Exception {
        //创建链接MySql
        java.sql.Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        //通过链接进行查询表中数据
        List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(mysqlConnection, "select * from gmall2024_config.table_process_dim", TableProcessDim.class, true);
        //将数据存入到hashmap中
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            hashMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        JdbcUtil.closeConnection(mysqlConnection);
    }

    //处理主流
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        //取出当前表名
        String table = jsonObject.getString("table");
        //获取状态中
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = readOnlyContext.getBroadcastState(mapDescriptor);
        //获取状态中的table值
        TableProcessDim tableProcessDim = broadcastState.get(table);
        if (null == tableProcessDim){
            tableProcessDim = hashMap.get(table);
        }
        if (tableProcessDim != null){
            collector.collect(Tuple2.of(jsonObject,tableProcessDim));
        }
    }

    //处理广播流
    @Override
    public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(mapDescriptor);
        broadcastState.put(tableProcessDim.getSourceTable(),tableProcessDim);
        String op = tableProcessDim.getOp();
        if ("d".equals(op)){
            broadcastState.remove(tableProcessDim.getSourceTable());
            hashMap.remove(tableProcessDim.getSourceTable());
        }
    }
}
