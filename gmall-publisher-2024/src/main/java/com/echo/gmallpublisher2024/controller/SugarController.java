package com.echo.gmallpublisher2024.controller;

import com.echo.gmallpublisher2024.service.GmvService;
import com.echo.gmallpublisher2024.service.UvService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

@RestController
public class SugarController {

    @Autowired
    private GmvService gmvService;

    @Autowired
    private UvService uvService;



    @RequestMapping("/test1")
    public String test1(){
        return  "success";
    }


    @RequestMapping("/getGmv")
    public String getGmv(@RequestParam(value = "date",defaultValue = "0") int date){

        if(date==0){
            date = getToday();
        }

        String gmv = gmvService.getGmv(date).toString();

        return "{" +
                " \"status\":0," +
                " \"msg\":\"\"," +
                " \"data\":" +gmv +
                "}";

    }

    @RequestMapping("/getUvByCh")
    public  String getUvByCh(@RequestParam(value = "date",defaultValue = "0")int date){
        if(date==0){
            date = getToday();
        }
        Map uvByCh = uvService.getUvByCh(date);
        Set set = uvByCh.keySet();
        Collection values = uvByCh.values();
        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\": [\n\"" +
                StringUtils.join(set,"\",\"") +
                "    \"],\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"独立访客\",\n" +
                "        \"data\": [\n" +
                StringUtils.join(values,",") +
                "        ]\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }

    private int getToday() {
        long currentTimeMillis = System.currentTimeMillis();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        return Integer.parseInt(dateFormat.format(currentTimeMillis));
    }
}
