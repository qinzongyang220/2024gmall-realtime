package com.bw.app;

import com.bw.base.BaseSqlApp;
import com.bw.common.Constant;
import com.bw.utils.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderRefund extends BaseSqlApp {
    public static void main(String[] args) {
        new DwdTradeOrderRefund().start(10017,4, Constant.TOPIC_DWD_TRADE_ORDER_REFUND);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String groupId) {
        //读取ods层
        readOdsDb(tableEnv,groupId);
        //创建字典表
        createBaseDic(tableEnv);
        //过滤出退单表
        extracted(tableEnv);
        //从订单中获取退单数据的省份id
        extracted1(tableEnv);
        //多表关联
        Table table = getTable(tableEnv);
        //写入kafka
        extracted2(tableEnv);
        table.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_REFUND).execute();
    }

    private static void extracted2(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql(
                "create table " + Constant.TOPIC_DWD_TRADE_ORDER_REFUND + "(" +
                        "id string," +
                        "user_id string," +
                        "order_id string," +
                        "sku_id string," +
                        "province_id string," +
                        "date_id string," +
                        "create_time string," +
                        "refund_type_code string," +
                        "refund_type_name string," +
                        "refund_reason_type_code string," +
                        "refund_reason_type_name string," +
                        "refund_reason_txt string," +
                        "refund_num string," +
                        "refund_amount string," +
                        "ts bigint " +
                        ")" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_ORDER_REFUND));
    }

    /**
     * 多表关联
     * @param tableEnv
     * @return
     */
    private static Table getTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery(
                "select " +
                        "ri.id," +
                        "ri.user_id," +
                        "ri.order_id," +
                        "ri.sku_id," +
                        "oi.province_id," +
                        "date_format(ri.create_time,'yyyy-MM-dd') date_id," +
                        "ri.create_time," +
                        "ri.refund_type," +
                        "dic1.info.dic_name," +
                        "ri.refund_reason_type," +
                        "dic2.info.dic_name," +
                        "ri.refund_reason_txt," +
                        "ri.refund_num," +
                        "ri.refund_amount," +
                        "ri.ts " +
                        "from order_refund_info ri " +
                        "join order_info oi " +
                        "on ri.order_id=oi.id " +
                        "join base_dic for system_time as of ri.proc_time as dic1 " +
                        "on ri.refund_type=dic1.rowkey " +
                        "join base_dic for system_time as of ri.proc_time as dic2 " +
                        "on ri.refund_reason_type=dic2.rowkey ");
    }

    /**
     * 从订单中获取退单数据的省份id
     * @param tableEnv
     */
    private static void extracted1(StreamTableEnvironment tableEnv) {
        Table table = tableEnv.sqlQuery(" select " +
                " `data`['id']  id ," +
                " `data`['province_id']  province_id " +
                "from topic_db " +
                "where `database`='gmall' " +
                " and `table`='order_info' " +
                " and ((`type` = 'bootstrap-insert' and `data`['order_status']='1005' )" +
                " or ( `type` = 'update' and `old`['order_status'] is not null  and `data`['order_status']='1005' )) ");
        tableEnv.createTemporaryView("order_info",table);
    }

    /**
     * 过滤出退单表
     * @param tableEnv
     */
    private static void extracted(StreamTableEnvironment tableEnv) {
        Table table = tableEnv.sqlQuery("select " +
                "data['id'] id," +
                "data['user_id'] user_id," +
                "data['order_id'] order_id," +
                "data['sku_id'] sku_id," +
                "data['refund_type'] refund_type," +
                "data['refund_num'] refund_num," +
                "data['refund_amount'] refund_amount," +
                "data['refund_reason_type'] refund_reason_type," +
                "data['refund_reason_txt'] refund_reason_txt," +
                "data['create_time'] create_time," +
                "proc_time," +
                "ts " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_refund_info' " +
                "and `type` in ('bootstrap-insert','insert') ");
        tableEnv.createTemporaryView("order_refund_info",table);
    }
}
