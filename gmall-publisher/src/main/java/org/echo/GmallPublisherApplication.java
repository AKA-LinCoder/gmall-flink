package org.echo;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication

@MapperScan(basePackages = "org.echo.mapper")
public class GmallPublisherApplication {
    public static void main(String[] args) {
        System.out.println("Hello world!");
    }
}