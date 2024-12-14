package com.bw.app;

import com.bw.base.BaseSqlApp;
import com.bw.common.Constant;
import com.bw.utils.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeRefundPaySucDetail extends BaseSqlApp {
    public static void main(String[] args) {
        new DwdTradeRefundPaySucDetail().start(10018,4, Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String groupId) {
        //读取ods
        readOdsDb(tableEnv,groupId);
        //创建字典表
        createBaseDic(tableEnv);
        //过滤出退款表
        extracted(tableEnv);
        //过滤出退单表中的退款成功信息
        extracted1(tableEnv);
        //过滤出订单表中的退款成功信息
        extracted2(tableEnv);
        //四 表关联
        Table table = getTable(tableEnv);
        //写入kafka
        extracted3(tableEnv);
        table.insertInto(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS).execute();
    }

    private static void extracted3(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS+"(" +
                "id string," +
                "user_id string," +
                "order_id string," +
                "sku_id string," +
                "province_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "date_id string," +
                "callback_time string," +
                "refund_num string," +
                "refund_amount string," +
                "ts bigint " +
                ")" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS));
    }

    /**
     * 四 表关联
     * @param tableEnv
     * @return
     */
    private static Table getTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select " +
                " rp.id," +
                " oi.user_id," +
                " rp.order_id," +
                " rp.sku_id," +
                " oi.province_id," +
                " rp.payment_type," +
                " dic.info.dic_name payment_type_name," +
                " date_format(rp.callback_time,'yyyy-MM-dd') date_id," +
                " rp.callback_time," +
                " ori.refund_num," +
                " rp.total_amount," +
                " rp.ts " +
                " from refund_payment rp " +
                " join order_refund_info ori " +
                " on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id " +
                " join order_info oi " +
                " on rp.order_id=oi.id " +
                " join base_dic for system_time as of rp.proc_time as dic " +
                " on rp.payment_type=dic.rowkey ");
    }

    /**
     * 过滤出订单表中的退款成功信息
     * @param tableEnv
     */
    private static void extracted2(StreamTableEnvironment tableEnv) {
        Table table = tableEnv.sqlQuery("select " +
                        " data['id'] id," +
                        " data['user_id'] user_id," +
                        " data['province_id'] province_id " +
                        " from topic_db " +
                        " where `database`='gmall' " +
                        " and `table`='order_info' " +
                        " and ((`type` = 'bootstrap-insert' and `data`['order_status']='1006' )" +
                        " or ( `type`='update' and `old`['order_status'] is not null and `data`['order_status']='1006' ) )");
        tableEnv.createTemporaryView("order_info", table);
    }

    /**
     * 过滤出退单表中的退款成功信息
     * @param tableEnv
     */
    private static void extracted1(StreamTableEnvironment tableEnv) {
        Table table = tableEnv.sqlQuery("select " +
                        " data['order_id'] order_id," +
                        " data['sku_id'] sku_id," +
                        " data['refund_num'] refund_num " +
                        " from topic_db " +
                        " where `database`='gmall' " +
                        " and `table`='order_refund_info' " +
                        " and ((`type` = 'bootstrap-insert' and `data`['refund_status']='0705' )" +
                        " or ( `type`='update' and `old`['refund_status'] is not null and `data`['refund_status']='0705' ) )");
        tableEnv.createTemporaryView("order_refund_info",table);
    }
    /**
     * 过滤退款表
     * @param tableEnv
     */
    private static void extracted(StreamTableEnvironment tableEnv) {
        Table table = tableEnv.sqlQuery("select" +
                " data['id'] id," +
                " data['order_id'] order_id," +
                " data['sku_id'] sku_id," +
                " data['payment_type'] payment_type," +
                " data['callback_time'] callback_time," +
                " data['total_amount'] total_amount," +
                " proc_time, " +
                " ts " +
                " from topic_db " +
                " where `database`='gmall' " +
                " and `table`='refund_payment' " +
                " and ((`type` = 'bootstrap-insert' and `data`['refund_status']='1602' )" +
                " or ( `type`='update' and `old`['refund_status'] is not null and `data`['refund_status']='1602' ) )");
        tableEnv.createTemporaryView("refund_payment",table);
    }
}
