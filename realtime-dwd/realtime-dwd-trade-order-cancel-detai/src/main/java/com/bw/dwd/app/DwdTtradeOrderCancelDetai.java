package com.bw.dwd.app;

import com.bw.base.BaseSqlApp;
import com.bw.common.Constant;
import com.bw.utils.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTtradeOrderCancelDetai extends BaseSqlApp {
    public static void main(String[] args) {
        new DwdTtradeOrderCancelDetai().start(10015,4, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String groupId) {
        //读取ods数据
        readOdsDb(tableEnv,groupId);
        //读取kafka中的订单详情表
        extracted(tableEnv, groupId);
        //过滤出取消的订单
        extracted(tableEnv);
        //关联
        Table table = getTable(tableEnv);
        //写入到kafka
        extracted1(tableEnv);
        table.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL).execute();
    }

    private static void extracted1(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_CANCEL + " (\n" +
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
                "  create_time  STRING,\n" +
                "  operate_time  STRING,\n" +
                "  split_total_amount  STRING,\n" +
                "  split_activity_amount  STRING,\n" +
                "  split_coupon_amount  STRING,\n" +
                "  ts bigint \n" +
                ")" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));
    }

    /**
     * 关联
     * @param tableEnv
     * @return
     */
    private static Table getTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select \n" +
                "  od.id,\n" +
                "  order_id,\n" +
                "  sku_id,\n" +
                "  user_id,\n" +
                "  province_id,\n" +
                "  activity_id,\n" +
                "  activity_rule_id,\n" +
                "  coupon_id,\n" +
                "  sku_name,\n" +
                "  order_price,\n" +
                "  sku_num,\n" +
                "  create_time,\n" +
                "  operate_time,\n" +
                "  split_total_amount,\n" +
                "  split_activity_amount,\n" +
                "  split_coupon_amount,\n" +
                "  oc.ts \n" +
                "from dwd_trade_order_detail od\n" +
                "join order_cancel oc  on od.order_id = oc.id ");
    }

    /**
     * 过滤出取消订单信息
     * @param tableEnv
     */
    private static void extracted(StreamTableEnvironment tableEnv) {
        Table table = tableEnv.sqlQuery(" select " +
                " `data`['id'] id, " +
                " `data`['operate_time'] operate_time, " +
                " ts " +
                " from topic_db " +
                " where `database`='gmall'\n" +
                " and `table`='order_info'\n" +
                " and ( (`type` in ('bootstrap-insert','insert') and `data`['order_status'] = '1003' )  or " +
                " (`type`='update' and `old`['order_status'] = '1001' and `data`['order_status'] = '1003' ))" );
        tableEnv.createTemporaryView("order_cancel",table);
    }

    /**
     * 获取kafka中的订单详情表
     * @param tableEnv
     * @param groupId
     */
    private static void extracted(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL + " (\n" +
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
                "  create_time  STRING,\n" +
                "  split_total_amount  STRING,\n" +
                "  split_activity_amount  STRING,\n" +
                "  split_coupon_amount  STRING,\n" +
                "  ts bigint\n" +
                " ) " + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, groupId));
    }
}
