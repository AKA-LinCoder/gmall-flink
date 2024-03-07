package com.echo.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

//自定义注解

//注解加到字段上
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.CLASS)
public @interface TransientSink {
}
