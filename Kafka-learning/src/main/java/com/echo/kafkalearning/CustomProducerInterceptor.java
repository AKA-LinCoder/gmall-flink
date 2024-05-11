package com.echo.kafkalearning;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 生产者流程：创建数据-》拦截器-》序列化-》分区器-》数据校验
 */
public class CustomProducerInterceptor {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");



        //TODO 自定义拦截器
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,ValueInterceptor.class.getName());
        //序列化配置
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        //重试机制会道之数据重复，数据乱序  -》幂等性解决，无论重试多少次，leader只保留一条

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        //发送信息
        for(int i=0;i<5;i++){
            kafkaProducer.send(new ProducerRecord<>("first","linguanyu"+i));
        }
        //关闭资源
        kafkaProducer.close();
    }
}