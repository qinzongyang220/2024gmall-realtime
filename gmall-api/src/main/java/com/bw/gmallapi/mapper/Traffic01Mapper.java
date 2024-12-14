package com.bw.gmallapi.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface Traffic01Mapper {

    @Select("select * from dws_traffic_source_keyword_page_view_window where cur_date=#{date}")
    public List<String> getDoris(String date);
}
