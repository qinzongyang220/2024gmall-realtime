package com.bw.dwd.app;

import com.bw.base.BaseSqlApp;
import com.bw.common.Constant;
import com.bw.utils.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderDetail extends BaseSqlApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10014,4, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String groupId) {
        //读取ods层数据
        readOdsDb(tableEnv,groupId);
        //数据提取
        //订单详情表
        filterOrderDetailInfo(tableEnv);
        //订单表
        filterOrderInfo(tableEnv);
        //活动表
        filterOrderActivity(tableEnv);
        //优惠卷表
        filterOrderCoupon(tableEnv);
        //关联表
        Table table = OrderJoin(tableEnv);
        //写入kafka
        createKafkaSinkTable(tableEnv);
        table.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL).execute();
    }

    /**
     * 创建kafka上的表
     * @param tableEnv
     */
    private void createKafkaSinkTable(StreamTableEnvironment tableEnv) {
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
                "  ts bigint,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED \n" +
                ")" + SQLUtil.getUpsertKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
        //因为有回撤流，所以要使用upsert-kafka 并且要指定主键
    }
    /**
     * 关联表
     * @param tableEnv
     * @return
     */
    private Table OrderJoin(StreamTableEnvironment tableEnv) {
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
                "  split_total_amount,\n" +
                "  split_activity_amount,\n" +
                "  split_coupon_amount,\n" +
                "  ts \n" +
                "from order_detail_info od\n" +
                "join order_info oi\n" +
                "on od.order_id = oi.id\n" +
                "left join order_detail_activity oda\n" +
                "on oda.id = od.id\n" +
                "left join order_detail_coupon odc\n" +
                "on odc.id = od.id ");
    }
    /**
     * 优惠劵表
     * @param tableEnv
     */
    private void filterOrderCoupon(StreamTableEnvironment tableEnv) {
        Table odcTable = tableEnv.sqlQuery("select \n" +
                "  `data`['order_detail_id'] id, \n" +
                "  `data`['coupon_id'] coupon_id\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail_coupon'\n" +
                "and `type` in ('bootstrap-insert','insert')");
        tableEnv.createTemporaryView("order_detail_coupon", odcTable);
    }
    /**
     * 活动表
     * @param tableEnv
     */
    private void filterOrderActivity(StreamTableEnvironment tableEnv) {
        Table odaTable = tableEnv.sqlQuery("select \n" +
                "  `data`['order_detail_id'] id, \n" +
                "  `data`['activity_id'] activity_id, \n" +
                "  `data`['activity_rule_id'] activity_rule_id\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail_activity'\n" +
                "and `type` in ('bootstrap-insert','insert')");
        tableEnv.createTemporaryView("order_detail_activity", odaTable);

    }
    /**
     * 订单表
     * @param tableEnv
     */
    private void filterOrderInfo(StreamTableEnvironment tableEnv) {
        Table oiTable = tableEnv.sqlQuery("select \n" +
                "  `data`['id'] id, \n" +
                "  `data`['user_id'] user_id, \n" +
                "  `data`['province_id'] province_id \n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_info'\n" +
                "and `type` in ('bootstrap-insert','insert')");
        tableEnv.createTemporaryView("order_info", oiTable);
    }
    /**
     * 订单详情表
     * @param tableEnv
     */
    private void filterOrderDetailInfo(StreamTableEnvironment tableEnv) {
        Table odTable = tableEnv.sqlQuery("select \n" +
                "  `data`['id'] id, \n" +
                "  `data`['order_id'] order_id, \n" +
                "  `data`['sku_id'] sku_id, \n" +
                "  `data`['sku_name'] sku_name, \n" +
                "  `data`['order_price'] order_price, \n" +
                "  `data`['sku_num'] sku_num, \n" +
                "  `data`['create_time'] create_time, \n" +
                "  `data`['split_total_amount'] split_total_amount, \n" +
                "  `data`['split_activity_amount'] split_activity_amount, \n" +
                "  `data`['split_coupon_amount'] split_coupon_amount, \n" +
                "  ts\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail'\n" +
                "and `type` in ('bootstrap-insert','insert')");
        tableEnv.createTemporaryView("order_detail_info", odTable);
    }
}
