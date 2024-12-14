package com.bw.functions;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Map;

public class DorisMapFunction <T> implements MapFunction<T,String> {
    @Override
    public String map(T t) throws Exception {
        SerializeConfig config = new SerializeConfig();
        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSONObject.toJSONString(t,config);
    }
}