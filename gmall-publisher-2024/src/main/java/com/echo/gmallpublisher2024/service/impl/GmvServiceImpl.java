package com.echo.gmallpublisher2024.service.impl;

import com.echo.gmallpublisher2024.mapper.GmvMapper;
import com.echo.gmallpublisher2024.service.GmvService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class GmvServiceImpl implements GmvService {


    @Autowired
    private GmvMapper gmvMapper;

    @Override
    public Double getGmv(int date) {
        return gmvMapper.selectGMV(date);
    }
}
