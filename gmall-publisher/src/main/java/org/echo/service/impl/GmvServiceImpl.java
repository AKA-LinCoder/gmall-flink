package org.echo.service.impl;

import org.echo.mapper.GmvMapper;
import org.echo.service.GmvService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
public class GmvServiceImpl implements GmvService {


    @Autowired
    private GmvMapper gmvMapper;

    @Override
    public Double getGmv(int date) {
        return gmvMapper.selectGMV(date);
    }
}
