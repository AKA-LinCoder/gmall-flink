package com.echo.gmallpublisher2024.mapper;

import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

public interface UvMapper {

    /**
     * 根据渠道分组，获取渠道独立访客数
     * @param date
     * @return
     */
    @Select("select ch,sum(uv_ct) uv,sum(uj_ct) uj from dws_traffic_vc_ch_ar_is_new_page_view_window where toYYYYMMDD(stt)=#{date} group by ch")
    List<Map> selectUvByCh(int date);
}
