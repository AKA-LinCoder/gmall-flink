package com.echo.gmallpublisher2024;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.echo.gmallpublisher2024.mapper")
public class GmallPublisher2024Application {

    public static void main(String[] args) {
        SpringApplication.run(GmallPublisher2024Application.class, args);
    }

}
