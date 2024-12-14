package com.bw.gmallapi.service.impl;

import com.bw.gmallapi.mapper.MySqlMapper;
import com.bw.gmallapi.pojo.KeyWord;
import com.bw.gmallapi.pojo.MapBean;
import com.bw.gmallapi.pojo.ShopBean;
import com.bw.gmallapi.service.MySqlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class SqlServiceImpl implements MySqlService{
    @Autowired
    MySqlMapper mySqlMapper;

    @Override
    public String getNumSum(String date) {
        return mySqlMapper.getNumSum(date);
    }

    @Override
    public List<KeyWord> getWordCount(String date) {
        return mySqlMapper.getWordCount(date);
    }

    @Override
    public List<MapBean> getMap() {
        return mySqlMapper.getMap();
    }

    @Override
    public List<ShopBean> getShopTop(String date) {
        return mySqlMapper.getShopTop(date);
    }
}
