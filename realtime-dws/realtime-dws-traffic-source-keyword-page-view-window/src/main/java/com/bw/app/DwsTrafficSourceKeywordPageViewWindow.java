package com.bw.app;

import com.bw.base.BaseSqlApp;
import com.bw.common.Constant;
import com.bw.functions.KwSplit;
import com.bw.utils.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow extends BaseSqlApp {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(10021,4, Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String groupId) {
        //读取page表
        extracted(tableEnv);
        //读取关键词
        extracted1(tableEnv);
        //创建自定义分词函数
        tableEnv.createTemporaryFunction("kwSplit", KwSplit.class);
        //炸开
        extracted2(tableEnv);
        //开窗
        Table table = getTable(tableEnv);
        //写到doris中
        extracted3(tableEnv);
        table.insertInto(Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW).execute();
    }

    private static void extracted3(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table "+ Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW +"(" +
                " window_start string," +
                " window_end string," +
                " cur_date string ," +
                " keyword string ," +
                " keyword_count bigint  " +
                ") " + SQLUtil.getDorisSinkSQL(Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW) + "");
    }

    /**
     * 开窗函数进行词频统计
     * @param tableEnv
     * @return
     */
    private Table getTable(StreamTableEnvironment tableEnv){
        return tableEnv.sqlQuery("select " +
                " cast(window_start as string) window_start," +
                " cast(window_end as string) window_end," +
                " cast(date_format(now(), 'yyyyMMdd') as string) cur_date," +
                " keyword," +
                " count(*) keyword_count" +
                " from " +
                " TABLE( TUMBLE(TABLE keyword_table, DESCRIPTOR(et), INTERVAL '5' second)) " +
                " GROUP BY window_start, window_end,keyword ");
    }
    /**
     * 炸开
     * @param tableEnv
     */
    private static void extracted2(StreamTableEnvironment tableEnv) {
        Table table = tableEnv.sqlQuery("select " +
                " keyword," +
                " et " +
                " from" +
                " kw_table join lateral table(kwSplit(kw)) on true ");
        tableEnv.createTemporaryView("keyword_table", table);
    }

    /**
     * 读取关键词
     * @param tableEnv
     */
    private static void extracted1(StreamTableEnvironment tableEnv) {
        Table table = tableEnv.sqlQuery("select " +
                " page['item'] kw," +
                " et " +
                " from page_log " +
                " where page['last_page_id'] in ('search','home') " +
                " and page['item_type']='keyword' and page['item'] is not null ");
        tableEnv.createTemporaryView("kw_table", table);
    }

    /**
     *读取page表
     * @param tableEnv
     */
    private static void extracted(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table page_log( " +
                " page map<string,string>, " +
                " ts bigint," +
                " et as to_timestamp_ltz(ts, 3), " +
                " watermark for et as et - interval '5' second ) "+
                SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRAFFIC_PAGE,Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW));
    }
}
