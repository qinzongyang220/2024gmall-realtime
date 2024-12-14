package com.bw.gmallapi.controller;

import com.bw.gmallapi.pojo.ColumnsBean;
import com.bw.gmallapi.pojo.KeyWord;
import com.bw.gmallapi.pojo.MapBean;
import com.bw.gmallapi.pojo.ShopBean;
import com.bw.gmallapi.service.MyOrderService;
import com.bw.gmallapi.service.MySqlService;
import com.bw.gmallapi.utils.DateFormatUtil;
import com.bw.gmallapi.utils.R;
import com.bw.gmallapi.utils.R2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;


@RestController
@RequestMapping("/gmall/api")
public class GmallController {
    @Autowired
    MyOrderService orderService;

    @RequestMapping("/traffic01")
    public String getTraffic01(@RequestParam(value = "date" ,defaultValue = "0")String date){
        if ("0".equals(date)){
            date = DateFormatUtil.getNowDate();
        }
        List<String> strings = orderService.Traffic01(date);
        return strings.toString();
    }

    @Autowired
    MySqlService sqlService;

    @RequestMapping("/get_numSum")
    public String getNumSum(@RequestParam(value = "date" ,defaultValue = "0")String date){
        if ("0".equals(date)){
            date = DateFormatUtil.getNowDate();
        }
        String num = sqlService.getNumSum(date);
        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": "+num+"\n" +
                "}";
    }


    @RequestMapping("/get_WordCount")
    public R<List<KeyWord>> getWordCount(@RequestParam(value = "date" ,defaultValue = "0")String date){
        if ("0".equals(date)){
            date = DateFormatUtil.getNowDate();
        }
        List<KeyWord> list = sqlService.getWordCount(date);
        return R.success(list);
    }

    @RequestMapping("/get_Map")
    public R<List<MapBean>> getMap(){
        List<MapBean> list = sqlService.getMap();
        return R.success(list);
    }

    @RequestMapping("/get_Shop")
    public R2<List<ShopBean>> getShop(@RequestParam(value = "date" ,defaultValue = "0")String date){
        if ("0".equals(date)){
            date = "2022-06-08 18:59:50";
        }

        List<ShopBean> list = sqlService.getShopTop(date);
        return R2.success(list);
    }
}
