package com.bw.functions;

import com.alibaba.fastjson.JSONObject;

public interface MyDim<T> {
    String getDimKey(T t);
    void setTable(T t, JSONObject jsonObject);
}
