package com.bw.functions;

import com.alibaba.fastjson.JSONObject;
import com.bw.bean.TradeSkuOrderBean;
import com.bw.common.Constant;
import com.bw.utils.HbaseUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;

public class Synchronous extends RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean> {
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
    public TradeSkuOrderBean map(TradeSkuOrderBean tradeSkuOrderBean) throws Exception {
        //链接dim_sku_info表中维度信息
        String skuId = tradeSkuOrderBean.getSkuId();
        JSONObject dimSkuInfo = HbaseUtil.getCells(hbaseConnect, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId);
        String spuId = dimSkuInfo.getString("spu_id");
        String category3Id = dimSkuInfo.getString("category3_id");
        String tmId = dimSkuInfo.getString("tm_id");
        tradeSkuOrderBean.setSpuId(spuId);
        tradeSkuOrderBean.setCategory3Id(category3Id);
        tradeSkuOrderBean.setTrademarkId(tmId);
        //链接dim_spu_info
        JSONObject dimSpuInfo = HbaseUtil.getCells(hbaseConnect, Constant.HBASE_NAMESPACE, "dim_spu_info", spuId);
        tradeSkuOrderBean.setSpuName(dimSpuInfo.getString("spu_name"));
        //链接dim_base_trademark
        JSONObject dimBaseTrademark = HbaseUtil.getCells(hbaseConnect, Constant.HBASE_NAMESPACE, "dim_base_trademark", tmId);
        tradeSkuOrderBean.setTrademarkName(dimBaseTrademark.getString("tm_name"));
        //链接dim_base_category3
        JSONObject dimBaseCategory3 = HbaseUtil.getCells(hbaseConnect, Constant.HBASE_NAMESPACE, "dim_base_category3", category3Id);
        String category2Id = dimBaseCategory3.getString("category2_id");
        tradeSkuOrderBean.setCategory3Name(dimBaseCategory3.getString("name"));
        tradeSkuOrderBean.setCategory2Id(category2Id);
        //链接dim_base_category2
        JSONObject dimBaseCategory2 = HbaseUtil.getCells(hbaseConnect, Constant.HBASE_NAMESPACE, "dim_base_category2", category2Id);
        String category1Id = dimBaseCategory2.getString("category1_id");
        tradeSkuOrderBean.setCategory2Name(dimBaseCategory2.getString("name"));
        tradeSkuOrderBean.setCategory1Id(category1Id);
        //链接dim_base_category1
        JSONObject dimBaseCategory1 = HbaseUtil.getCells(hbaseConnect, Constant.HBASE_NAMESPACE, "dim_base_category1", category1Id);
        tradeSkuOrderBean.setCategory1Name(dimBaseCategory1.getString("name"));
        return tradeSkuOrderBean;
    }
}
