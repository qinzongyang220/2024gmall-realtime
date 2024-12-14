package com.bw.dwd.app;

import com.bw.base.BaseSqlApp;
import com.bw.common.Constant;
import com.bw.utils.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd extends BaseSqlApp {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013,4, Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String groupId) {
        //读取ods层
        readOdsDb(tableEnv,groupId);
        //过滤数据
        Table table = getTable(tableEnv);
        //建表
        getTableResult(tableEnv);
        //写入kafka
        table.insertInto(Constant.TOPIC_DWD_TRADE_CART_ADD).execute();
    }

    /**
     * 建表
     * @param tableEnv
     */
    private void getTableResult(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_CART_ADD + "(\n" +
                " id  STRING,\n" +
                " user_id STRING,\n" +
                " sku_id STRING,\n" +
                " cart_price STRING,\n" +
                " sku_num BIGINT,\n" +
                " sku_name STRING,\n" +
                " is_checked STRING,\n" +
                " create_time STRING,\n" +
                " operate_time STRING,\n" +
                " is_ordered STRING,\n" +
                " order_time STRING,\n" +
                "   ts BIGINT)" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_CART_ADD));
    }
    /**
     * 过滤字段，提取出加购表
     * @param tableEnv
     * @return
     */
    private static Table getTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select " +
                " `data`['id'] id, " +
                " `data`['user_id'] user_id, " +
                " `data`['sku_id'] sku_id, " +
                " `data`['cart_price'] cart_price, " +
                "  if(`type` in ('bootstrap-insert','insert'),cast(`data`['sku_num'] as bigint),cast(`data`['sku_num'] as bigint)-cast(`old`['sku_num'] as bigint)) sku_num, " +
                " `data`['sku_name'] sku_name, " +
                " `data`['is_checked'] is_checked, " +
                " `data`['create_time'] create_time, " +
                " `data`['operate_time'] operate_time, " +
                " `data`['is_ordered'] is_ordered, " +
                " `data`['order_time'] order_time," +
                "  ts " +
                " from topic_db " +
                " where `database`='gmall' " +
                " and `table`='cart_info' " +
                " and (`type` in ('bootstrap-insert','insert') or (`type`='update' and `old`['sku_num'] is not null " +
                " and cast(`data`['sku_num'] as bigint) > cast(`old`['sku_num'] as bigint) ) ) ");
    }
}
