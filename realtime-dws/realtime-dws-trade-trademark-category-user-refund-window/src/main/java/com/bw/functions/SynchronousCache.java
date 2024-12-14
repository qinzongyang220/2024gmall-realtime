package com.bw.functions;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.bean.TradeTrademarkCategoryUserRefundBean;
import com.bw.common.Constant;
import com.bw.utils.HbaseUtil;
import com.bw.utils.RedisUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import java.io.IOException;

public class SynchronousCache extends RichMapFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean> {
    Connection hbaseConnect = null;
    Jedis jedis = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConnect = HbaseUtil.getHbaseConnect();
        jedis = RedisUtil.getJedis();
    }

    @Override
    public void close() throws Exception {
        HbaseUtil.closeHBaseConn(hbaseConnect);
        RedisUtil.closeJedis(jedis);
    }

    public JSONObject getData(String table,String key) throws IOException {
        //获取redis的key
        String redisKey = RedisUtil.getRedisKey(table, key);
        //获取缓存中的数据
        String redisData = jedis.get(redisKey);
        //初始化返回的数据
        JSONObject jsonObject = null;
        if (!redisData.isEmpty()){
            jsonObject = JSON.parseObject(redisData);
        }else {
            jsonObject = HbaseUtil.getCells(hbaseConnect, Constant.HBASE_NAMESPACE, table, key);
            jedis.setex(redisKey,24*3600,jsonObject.toJSONString());
        }
        return jsonObject;
    }

    @Override
    public TradeTrademarkCategoryUserRefundBean map(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean) throws Exception {
        //获取sku_id用户关联dim_sku_info
        String skuId = tradeTrademarkCategoryUserRefundBean.getSkuId();
        //链接dim_sku_info表中维度信息
        JSONObject dim_sku_info = getData("dim_sku_info", skuId);
        //写到实体类中
        String category3Id = dim_sku_info.getString("category3_id");
        String tmId = dim_sku_info.getString("tm_id");
        tradeTrademarkCategoryUserRefundBean.setCategory3Id(category3Id);
        tradeTrademarkCategoryUserRefundBean.setTrademarkId(tmId);

        //链接dim_base_trademark
        JSONObject dim_base_trademark = getData("dim_base_trademark", tmId);
        //写到实体类中
        tradeTrademarkCategoryUserRefundBean.setTrademarkName(dim_base_trademark.getString("tm_name"));

        //链接dim_base_category3
        JSONObject dim_base_category3 = getData("dim_base_category3", category3Id);
        //写到实体类中
        String category2Id = dim_base_category3.getString("category2_id");
        tradeTrademarkCategoryUserRefundBean.setCategory3Name(dim_base_category3.getString("name"));
        tradeTrademarkCategoryUserRefundBean.setCategory2Id(category2Id);

        //链接dim_base_category2
        JSONObject dim_base_category2 = getData("dim_base_category2", category2Id);
        //写到实体类中
        String category1Id = dim_base_category2.getString("category1_id");
        tradeTrademarkCategoryUserRefundBean.setCategory2Name(dim_base_category2.getString("name"));
        tradeTrademarkCategoryUserRefundBean.setCategory1Id(category1Id);

        //链接dim_base_category1
        JSONObject dim_base_category1 = getData("dim_base_category1", category1Id);
        //写到实体类中
        tradeTrademarkCategoryUserRefundBean.setCategory1Name(dim_base_category1.getString("name"));

        return tradeTrademarkCategoryUserRefundBean;
    }
}
