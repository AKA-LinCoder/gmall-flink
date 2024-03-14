package org.echo.controller;

import org.echo.service.GmvService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;

@RestController
public class SugarController {

    @Autowired
    private GmvService gmvService;

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

    private int getToday() {
        long currentTimeMillis = System.currentTimeMillis();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        return Integer.parseInt(dateFormat.format(currentTimeMillis));
    }
}
