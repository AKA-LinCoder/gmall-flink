package com.echo.gmallpublisher2024.service.impl;

import com.echo.gmallpublisher2024.mapper.UvMapper;
import com.echo.gmallpublisher2024.service.UvService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class UvServiceImpl implements UvService {

    @Autowired
    private UvMapper uvMapper;

    @Override
    public Map getUvByCh(int date) {
        //创建hashmap用户存放结果数据
        HashMap<String, BigInteger> resultMap = new HashMap<>();
        //查询clickhouse获取数据
        List<Map> mapList = uvMapper.selectUvByCh(date);
        //遍历集合，取出渠道和日活数据放入结果集
        for (Map map : mapList) {
            resultMap.put((String) map.get("ch"),(BigInteger) map.get("uv"));
        }
        return resultMap;
    }
}
