package com.bw.dwd.app;

import com.bw.base.BaseSqlApp;
import com.bw.common.Constant;
import com.bw.utils.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CommonApp extends BaseSqlApp {
    public static void main(String[] args) {
        new CommonApp().start(10012,4, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String groupId) {
        //读取ods层的数据
        readOdsDb(tableEnv,groupId);
//        tableEnv.sqlQuery("select * from topic_db").execute().print();
        //数据过滤
        Table commentTable = getCommentTable(tableEnv);
        //创建临时表
        tableEnv.createTemporaryView("comment_info",commentTable);
        //读取码表
        createBaseDic(tableEnv);
        //关联数据
        Table table = getTable(tableEnv);
        //创建kafka表
        extracted(tableEnv);
        //写入到kafka
        table.insertInto(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO).execute();
    }

    private static void extracted(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql(" create table " + Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO +" ( " +
                " id string," +
                " user_id string," +
                " nick_name string," +
                " sku_id string," +
                " spu_id string," +
                " order_id string," +
                " appraise string," +
                " dic_name string," +
                " comment_txt string," +
                " create_time string" +
                " )" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
    }

    private static Table getTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select " +
                " id," +
                " user_id," +
                " nick_name," +
                " sku_id," +
                " spu_id," +
                " order_id," +
                " appraise," +
                " info.dic_name," +
                " comment_txt," +
                " create_time " +
                " from comment_info c " +
                " join base_dic for system_time as of c.proc_time as b " +
                " on c.appraise = b.rowkey ");
    }

    private static Table getCommentTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select " +
                " `data`['id'] id," +
                " `data`['user_id'] user_id," +
                " `data`['nick_name'] nick_name," +
                " `data`['sku_id'] sku_id," +
                " `data`['spu_id'] spu_id," +
                " `data`['order_id'] order_id," +
                " `data`['appraise'] appraise," +
                " `data`['comment_txt'] comment_txt," +
                " `data`['create_time'] create_time," +
                " proc_time " +
                " from topic_db " +
                " where `database`='gmall' " +
                " and `table`='comment_info' " +
                " and `type` in ('bootstrap-insert','insert') ");
    }
}
