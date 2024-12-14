package com.bw.gmallapi.service;

import com.bw.gmallapi.pojo.KeyWord;
import com.bw.gmallapi.pojo.MapBean;
import com.bw.gmallapi.pojo.ShopBean;

import java.util.List;

public interface MySqlService {

    String getNumSum(String date);

    List<KeyWord> getWordCount(String date);

    List<MapBean> getMap();

    List<ShopBean> getShopTop(String date);
}
