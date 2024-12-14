package com.bw.functions;

import com.bw.bean.ShopBean;
import com.bw.utils.JdbcUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class MySink extends RichSinkFunction<ShopBean>{
    private Connection mysqlConnection;
    private PreparedStatement ps;
    @Override
    public void open(Configuration parameters) throws Exception {
        mysqlConnection = JdbcUtil.getMysqlConnection();
        ps = mysqlConnection.prepareStatement("insert into Test.shop values (?,?,?,?,?,?)");
    }

    @Override
    public void close() throws Exception {
        mysqlConnection.close();
    }

    @Override
    public void invoke(ShopBean value, SinkFunction.Context context) throws Exception {
        ps.setString(1,value.getStt());
        ps.setString(2,value.getEdt());
        ps.setString(3,value.getCurDate());
        ps.setBigDecimal(4,value.getAmount());
        ps.setString(5,value.getShopId());
        ps.setString(6,value.getShopName());
        ps.executeUpdate();
    }
}

