package com.echo.gmallpublisher2024.mapper;

import org.apache.ibatis.annotations.Select;

public interface GmvMapper {
    //查询clickhouse
    @Select("select sum(order_amount) from dws_trade_province_order_window where toYYYYMMDD(stt)=#{date}})")
    Double selectGMV(int date);
}
