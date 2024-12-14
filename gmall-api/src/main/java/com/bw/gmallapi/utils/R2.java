package com.bw.gmallapi.utils;

import com.bw.gmallapi.pojo.ColumnsBean;
import com.bw.gmallapi.pojo.TT;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class R2<T> implements Serializable {

    private Integer status; //编码：0成功，-1和其它数字为失败

    private String msg; //错误信息

    private TT data; //数据

    private Map map = new HashMap(); //动态数据

    public static <T> R2<T> success(T object) {
        R2<T> r = new R2<T>();
        ColumnsBean c1 = new ColumnsBean("开始时间","stt");
        ColumnsBean c2 = new ColumnsBean("结束时间","edt");
        ColumnsBean c3 = new ColumnsBean("店铺名称","shopName");
        ColumnsBean c4 = new ColumnsBean("金额","ammount");
        ColumnsBean c5 = new ColumnsBean("排名","num");
        List<ColumnsBean> list = new ArrayList<>();
        list.add(c1);
        list.add(c2);
        list.add(c3);
        list.add(c4);
        list.add(c5);
        r.data = new TT(list,object);
        r.status = 0;
        return r;
    }
    public static <T> R2<T> error(String msg) {
        R2 r = new R2();
        r.msg = msg;
        r.status = -1;
        return r;
    }

    public R2<T> add(String key, Object value) {
        this.map.put(key, value);
        return this;
    }

}
