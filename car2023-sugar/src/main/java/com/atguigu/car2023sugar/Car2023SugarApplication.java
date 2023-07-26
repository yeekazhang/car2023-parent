package com.atguigu.car2023sugar;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.car2023sugar.mapper")
public class Car2023SugarApplication {

    public static void main(String[] args) {
        SpringApplication.run(Car2023SugarApplication.class, args);
    }

}
