package com.bw.gmallapi.service.impl;

import com.bw.gmallapi.mapper.Traffic01Mapper;
import com.bw.gmallapi.service.MyOrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class OrderServiceImpl implements MyOrderService {

    @Autowired
    Traffic01Mapper traffic01Mapper;

    @Override
    public List<String> Traffic01(String date) {
        return traffic01Mapper.getDoris(date);
    }
}
