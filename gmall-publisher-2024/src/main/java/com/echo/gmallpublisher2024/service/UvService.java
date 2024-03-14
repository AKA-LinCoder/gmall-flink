package com.echo.gmallpublisher2024.service;

import java.util.List;
import java.util.Map;

public interface UvService {

    /**
     * 根据渠道分组，获取渠道独立访客数
     * @param date
     * @return
     */
    Map getUvByCh(int date);
}
