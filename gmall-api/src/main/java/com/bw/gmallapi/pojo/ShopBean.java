package com.bw.gmallapi.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class ShopBean {
    String stt;
    String edt;
    String shopName;
    String ammount;
    Integer num;
}
