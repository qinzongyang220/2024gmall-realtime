package com.bw.functions;

import com.alibaba.fastjson.JSONObject;
import com.bw.bean.TradeProvinceOrderBean;
import com.bw.common.Constant;
import com.bw.utils.HbaseUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;

public class Synchronous extends RichMapFunction<TradeProvinceOrderBean, TradeProvinceOrderBean> {
    Connection hbaseConnect = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConnect = HbaseUtil.getHbaseConnect();
    }

    @Override
    public void close() throws Exception {
        if (hbaseConnect==null)
            hbaseConnect.close();
    }

    @Override
    public TradeProvinceOrderBean map(TradeProvinceOrderBean tradeProvinceOrderBean) throws Exception {
        //获取省份Id
        String provinceId = tradeProvinceOrderBean.getProvinceId();
        //链接dim_base_category1
        JSONObject dimBaseProvince = HbaseUtil.getCells(hbaseConnect, Constant.HBASE_NAMESPACE, "dim_base_province", provinceId);
        tradeProvinceOrderBean.setProvinceName(dimBaseProvince.getString("name"));
        return tradeProvinceOrderBean;
    }
}
