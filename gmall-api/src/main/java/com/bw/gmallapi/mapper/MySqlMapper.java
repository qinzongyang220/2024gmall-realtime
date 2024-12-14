package com.bw.gmallapi.mapper;

import com.bw.gmallapi.pojo.KeyWord;
import com.bw.gmallapi.pojo.MapBean;
import com.bw.gmallapi.pojo.ShopBean;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface MySqlMapper {
    @Select("select sum(`value`) from NumSum where car_date=#{date}")
    String getNumSum(String date);

    @Select("select name,`value` from WordCount where car_date=#{date}")
    List<KeyWord> getWordCount(String date);

    @Select("select name,`value` from Map")
    List<MapBean> getMap();

//    @Select("select stt,edt,shopName,ammount, row_number () over (partition by stt,edt order by ammount desc)  from shop ")
    @Select("select stt,edt,shopName,ammount,(@row_number := @row_number + 1) AS num from shop,(SELECT @row_number := 0) AS init where stt = #{date} order by ammount desc limit 10")
    List<ShopBean> getShopTop(String date);
}
