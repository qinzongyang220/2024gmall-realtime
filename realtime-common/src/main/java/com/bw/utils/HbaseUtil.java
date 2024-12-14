package com.bw.utils;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Slf4j
public class HbaseUtil {

    // 获取链接
    public static Connection getHbaseConnect() throws IOException {
//        Configuration conf = new Configuration();
//        conf.set("hbase.zookeeper.quorum", "hadoop102");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        return ConnectionFactory.createConnection(conf);
        return ConnectionFactory.createConnection();
    }

    // 关闭链接
    public static  void closeHBaseConn(Connection conn) throws IOException {
        if (conn != null){
            conn.close();
        }
    }

    /**
     * 获取到 Hbase 的异步连接
     *
     * @return 得到异步连接对象
     */
    public static AsyncConnection getHBaseAsyncConnection() {
//        Configuration conf = new Configuration();
//        conf.set("hbase.zookeeper.quorum", "hadoop102");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            return ConnectionFactory.createAsyncConnection().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void closeAsyncConnection(AsyncConnection asyncConnection ){
        if (asyncConnection != null && !asyncConnection.isClosed()){
            try {
                asyncConnection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建表
     * @param hbaseConn
     * @param nameSpace
     * @param table
     * @param families
     * @throws IOException
     */
    public static void createHBaseTable(Connection hbaseConn,
                                        String nameSpace,
                                        String table,
                                        String ...families) throws IOException {
        Admin admin = hbaseConn.getAdmin();
        TableName tableName = TableName.valueOf(nameSpace, table);
        // 判断要建的表是否存在
        if (admin.tableExists(tableName)) {
            return;
        }


//        // 列族描述器
//        ColumnFamilyDescriptor cfDesc = ColumnFamilyDescriptorBuilder.of(family);
//        // 表的描述器
//        TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
//                .setColumnFamily(cfDesc) // 给表设置列族
//                .build();
//        // 创建表
//        admin.createTable(desc);


        // 2. 创建表格描述
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(nameSpace, table));

        for (String family : families) {
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family))
                    .build();
            tableDescriptorBuilder.setColumnFamily(familyDescriptor);
        }

        // 3. 使用admin调用方法创建表格
        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            System.out.println("e = " + e);
            System.out.println("当前表格已经存在  不需要重复创建" + nameSpace + ":" + table);
        }
        admin.close();
        log.info(nameSpace + " " + table + " 建表成功");
    }
    // 删除表
    public static void dropHBaseTable(Connection hbaseConn,
                                      String nameSpace,
                                      String table) throws IOException {

        Admin admin = hbaseConn.getAdmin();
        TableName tableName = TableName.valueOf(nameSpace, table);
        if (admin.tableExists(tableName)) {
            // 先禁用
            admin.disableTable(tableName);
            // 才能删除
            admin.deleteTable(tableName);
        }
        admin.close();
        log.info(nameSpace + " " + table + " 删除成功");

    }

    /**
     * 写数据到HBase
     * @param connection 一个同步连接
     * @param namespace  命名空间
     * @param tableName  表名
     * @param rowKey    主键
     * @param family    列族名
     * @param data     列名和列值  jsonObj对象
     * @throws IOException
     */
    public static void putCells(Connection connection, String namespace, String tableName, String rowKey, String family, JSONObject data) throws IOException {
        // 1. 获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2. 创建写入对象
        Put put = new Put(Bytes.toBytes(rowKey));

        for (String column : data.keySet()) {
            String columnValue = data.getString(column);
            if (columnValue != null){
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(columnValue));
            }
        }

        // 3. 调用方法写出数据
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 4. 关闭table
        table.close();
    }

    /**
     * 删除一整行数据
     * @param connection  一个同步连接
     * @param namespace   命名空间名称
     * @param tableName   表格
     * @param rowKey     主键
     * @throws IOException
     */
    public static void deleteCells(Connection connection, String namespace, String tableName, String rowKey) throws IOException {
        // 1. 获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2. 创建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        // 3. 调用方法删除数据
        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 4. 关闭table
        table.close();
    }

    /**
     * 获取数据
     * @param connection
     * @param namespace
     * @param tableName
     * @param rowKey
     * @return
     * @throws IOException
     */
    public static JSONObject getCells(Connection connection,String namespace,String tableName,String rowKey) throws IOException {
        // 1. 获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2. 创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        JSONObject jsonObject = new JSONObject();
        // 3. 调用get方法
        try {
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                jsonObject.put(new String(CellUtil.cloneQualifier(cell), StandardCharsets.UTF_8),new String(CellUtil.cloneValue(cell),StandardCharsets.UTF_8));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        // 4. 关闭table
        table.close();

        return jsonObject;
    }

    /**
     * 获取异步数据
     * @param hBaseAsyncConnection
     * @param namespace
     * @param tableName
     * @param rowKey
     * @return
     * @throws IOException
     */
    public static JSONObject getAsyncCells(AsyncConnection hBaseAsyncConnection,String namespace,String tableName,String rowKey) throws IOException {
        // 1. 获取table
        AsyncTable<AdvancedScanResultConsumer> table = hBaseAsyncConnection.getTable(TableName.valueOf(namespace, tableName));

        // 2. 创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        JSONObject jsonObject = new JSONObject();
        try {
            // 3. 调用get方法
            Result result = table.get(get).get();
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                jsonObject.put(Bytes.toString(CellUtil.cloneQualifier(cell)),Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }catch (Exception e){
            e.printStackTrace();
        }


        return jsonObject;
    }
}
