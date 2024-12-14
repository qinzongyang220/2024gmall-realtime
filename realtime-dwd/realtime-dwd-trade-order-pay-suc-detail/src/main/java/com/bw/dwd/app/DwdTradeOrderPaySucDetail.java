package com.bw.dwd.app;

import com.bw.base.BaseSqlApp;
import com.bw.common.Constant;
import com.bw.utils.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderPaySucDetail extends BaseSqlApp {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(10016,4, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String groupId) {
        //读取ods层
        readOdsDb(tableEnv,groupId);
        //读取字典表
        createBaseDic(tableEnv);
        //读取下单业务实时表
        extracted(tableEnv, groupId);
        //过滤支付成功数据
        extracted(tableEnv);
        //订单表和支付表做 interval join关联
        extracted1(tableEnv);
        //关联字典表
        Table table = getTable(tableEnv);
        //写入kafka
        extracted2(tableEnv);
        table.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS).execute();
    }

    private static void extracted2(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS + "(\n" +
                "  id  STRING,\n" +
                "  order_id  STRING,\n" +
                "  sku_id  STRING,\n" +
                "  user_id  STRING,\n" +
                "  province_id  STRING,\n" +
                "  activity_id  STRING,\n" +
                "  activity_rule_id  STRING,\n" +
                "  coupon_id  STRING,\n" +
                "  sku_name  STRING,\n" +
                "  order_price  STRING,\n" +
                "  sku_num  STRING,\n" +
                "  split_total_amount  STRING,\n" +
                "  split_activity_amount  STRING,\n" +
                "  split_coupon_amount  STRING,\n" +
                "  payment_type_code  STRING,\n" +
                "  payment_type_name  STRING,\n" +
                "  payment_time  STRING,\n" +
                "  ts  BIGINT\n" +
                ")" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));
    }

    private static Table getTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select " +
                "  id,  " +
                "  order_id,  " +
                "  sku_id,  " +
                "  user_id,  " +
                "  province_id,  " +
                "  activity_id,  " +
                "  activity_rule_id,  " +
                "  coupon_id,  " +
                "  sku_name,  " +
                "  order_price,  " +
                "  sku_num,  " +
                "  split_total_amount,  " +
                "  split_activity_amount,  " +
                "  split_coupon_amount,  " +
                "  payment_type payment_type_code,  " +
                "  info.dic_name payment_type_name,  " +
                "  payment_time,  " +
                "  ts  " +
                " from pay_order po " +
                " join base_dic for system_time as of po.proc_time as d " +
                " on po.payment_type = d.rowkey ");
    }

    private static void extracted1(StreamTableEnvironment tableEnv) {
        Table table = tableEnv.sqlQuery(" select " +
                " od.id," +
                " od.order_id, " +
                " sku_id, " +
                " pi.user_id, " +
                " province_id, " +
                " activity_id, " +
                " activity_rule_id, " +
                " coupon_id, " +
                " sku_name, " +
                " order_price, " +
                " sku_num, " +
                " split_total_amount, " +
                " split_activity_amount, " +
                " split_coupon_amount, " +
                " pi.ts, " +
                " payment_type, " +
                " callback_time payment_time, " +
                " proc_time      " +
                " from " +
                " dwd_trade_order_detail od , payment_info pi" +
                " where od.order_id = pi.order_id" +
                " and pi.row_time between od.row_time - INTERVAL '15' SECOND AND pi.row_time + INTERVAL '5' SECOND ");
        tableEnv.createTemporaryView("pay_order",table);
    }

    /**
     * 支付成功表
     * @param tableEnv
     */
    private static void extracted(StreamTableEnvironment tableEnv) {
        Table table = tableEnv.sqlQuery("select " +
                " `data`['id'] id,   " +
                " `data`['order_id'] order_id,   " +
                " `data`['user_id'] user_id,   " +
                " `data`['payment_type'] payment_type,   " +
                " `data`['callback_time'] callback_time,   " +
                "  ts,   " +
                "  row_time,   " +
                "  proc_time   " +
                " from topic_db " +
                " where `database` = 'gmall'   " +
                " and `table` = 'payment_info'     " +
                " and ((`type`='bootstrap-insert' and `data`['payment_status'] = '1602' ) " +
                " or (`type` = 'update' and `old`['payment_status'] is not null " +
                "and `data`['payment_status'] = '1602') )");
        tableEnv.createTemporaryView("payment_info",table);
    }

    /**
     * 订单详情表
     * @param tableEnv
     * @param groupId
     */
    private static void extracted(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL + " (   " +
                "  id  STRING,   " +
                "  order_id  STRING,   " +
                "  sku_id  STRING,   " +
                "  user_id  STRING,   " +
                "  province_id  STRING,   " +
                "  activity_id  STRING,   " +
                "  activity_rule_id  STRING,   " +
                "  coupon_id  STRING,   " +
                "  sku_name  STRING,   " +
                "  order_price  STRING,   " +
                "  sku_num  STRING,   " +
                "  create_time  STRING,   " +
                "  split_total_amount  STRING,   " +
                "  split_activity_amount  STRING,   " +
                "  split_coupon_amount  STRING,   " +
                "  ts bigint,   " +
                "  row_time as TO_TIMESTAMP_LTZ(ts, 0) ,   " +
                "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND    " +
                ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, groupId));
    }
}
