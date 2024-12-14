package com.bw.functions;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.common.Constant;
import com.bw.utils.HbaseUtil;
import com.bw.utils.RedisUtil;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class Async<T>extends RichAsyncFunction<T, T> implements MyDim<T>{
    private AsyncConnection hBaseAsyncConnection;
    private StatefulRedisConnection<String, String> redisAsyncConnection;
    @Override
    public void open(Configuration parameters) throws Exception {
        hBaseAsyncConnection = HbaseUtil.getHBaseAsyncConnection();
        redisAsyncConnection = RedisUtil.getRedisAsyncConnection();
    }

    @Override
    public void close() throws Exception {
        hBaseAsyncConnection.close();
        redisAsyncConnection.close();
    }

    private String table;

    public Async(String table) {
        this.table = table;
    }

    @Override
    public void asyncInvoke(T t, ResultFuture<T> resultFuture) throws Exception {
        String dimKey = getDimKey(t);
        String redisKey = RedisUtil.getRedisKey(table, dimKey);

        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                String dimIfo = null;
                try {
                    RedisFuture<String> stringRedisFuture = redisAsyncConnection.async().get(redisKey);
                    dimIfo = stringRedisFuture.get();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
                return dimIfo;
            }
        }).thenApplyAsync(new Function<String, JSONObject>() {
            @Override
            public JSONObject apply(String dimIfo) {
                JSONObject jsonObject = null;
                if (dimIfo==null || "".equals(dimIfo) ){
                    try {
                        jsonObject = HbaseUtil.getAsyncCells(hBaseAsyncConnection, Constant.HBASE_NAMESPACE, table, dimKey);
                        redisAsyncConnection.async().setex(redisKey,24*3600,jsonObject.toJSONString());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }else {
                    jsonObject = JSON.parseObject(dimIfo);
                }
                return jsonObject;
            }
        }).thenAccept(new Consumer<JSONObject>() {
            @Override
            public void accept(JSONObject jsonObject) {
                if (jsonObject.isEmpty()){
                    System.out.println("没有查到维度数据:" + table + ":" + dimKey);
                }else {
                    setTable(t,jsonObject);
                }
                resultFuture.complete(Collections.singletonList(t));
            }
        });
    }

}