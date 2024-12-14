package com.bw.dim.functions;

import com.alibaba.fastjson.JSONObject;
import com.bw.bean.TableProcessDim;
import com.bw.common.Constant;
import com.bw.utils.HbaseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

public class DimSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    private Connection hbaseConnect;
    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConnect = HbaseUtil.getHbaseConnect();
    }

    @Override
    public void close() throws Exception {
        HbaseUtil.closeHBaseConn(hbaseConnect);
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
        //主流数据
        JSONObject f0 = value.f0;
        //配置数据
        TableProcessDim f1 = value.f1;
        String sinkTable = f1.getSinkTable();
        String sinkRowKey = f1.getSinkRowKey();
        String sinkFamily = f1.getSinkFamily();
        JSONObject data = f0.getJSONObject("data");

        String type = data.getString("type");
        String rowKey = data.getString(sinkRowKey);

        if ("delete".equals(type)){
            HbaseUtil.deleteCells(hbaseConnect, Constant.HBASE_NAMESPACE,sinkTable,rowKey);
        }else {
            HbaseUtil.putCells(hbaseConnect, Constant.HBASE_NAMESPACE,sinkTable,rowKey,sinkFamily,data);
        }
    }
}
