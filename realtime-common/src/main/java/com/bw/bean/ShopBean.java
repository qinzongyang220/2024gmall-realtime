package com.bw.bean;


import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class ShopBean {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    // 当天日期
    String curDate;
    //下单金额
    BigDecimal amount;
    //店铺id
    String shopId;
    //店铺名称
    String shopName;
    //时间戳
    @JSONField(serialize = false)
    Long ts;
}
