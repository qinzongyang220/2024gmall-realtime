package com.bw.functions;

import com.alibaba.fastjson.JSONObject;
import com.bw.bean.TableProcessDwd;
import com.bw.utils.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;

public class DwdProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>> {
    private final MapStateDescriptor<String, TableProcessDwd> mapDescriptor;
    private HashMap<String, TableProcessDwd> hashMap = new HashMap<>();


    public DwdProcessFunction(MapStateDescriptor<String, TableProcessDwd> mapDescriptor) {
        this.mapDescriptor = mapDescriptor;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        // 手动读取配置表
        Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDwd> tableProcessDwds = JdbcUtil.queryList(mysqlConnection, "select * from gmall2024_config.table_process_dwd", TableProcessDwd.class, true);
        for (TableProcessDwd tableProcessDwd : tableProcessDwds) {
            String type = tableProcessDwd.getSourceType();
            if ("bootstrap-insert".equals(type)){
                type = "insert";
            }
            hashMap.put(tableProcessDwd.getSourceTable()+"-"+type, tableProcessDwd);
        }
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = readOnlyContext.getBroadcastState(mapDescriptor);
        String table = jsonObject.getString("table");
        String type = jsonObject.getString("type");
        // user_info-insert
        // user_info-update
        if ("bootstrap-insert".equals(type)){
            type = "insert";
        }
        String key = table+"-"+type;

        TableProcessDwd tableProcessDwd = broadcastState.get(key);

        if (tableProcessDwd == null) {
            tableProcessDwd = hashMap.get(key);
        }
        if (tableProcessDwd != null) {
            collector.collect(Tuple2.of(jsonObject, tableProcessDwd));
        }
    }

    @Override
    public void processBroadcastElement(TableProcessDwd tableProcessDwd, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        BroadcastState<String, TableProcessDwd> broadcastState = context.getBroadcastState(mapDescriptor);
        String op = tableProcessDwd.getOp();
        String sourceType = tableProcessDwd.getSourceType();
        // user_info-insert
        String key = tableProcessDwd.getSourceTable()+"-"+sourceType;
        if ("d".equals(op)) {
            broadcastState.remove(key);
            hashMap.remove(key);
        } else {
            broadcastState.put(key, tableProcessDwd);
        }

    }
}

