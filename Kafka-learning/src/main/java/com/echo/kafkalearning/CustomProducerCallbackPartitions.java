package com.echo.kafkalearning;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerCallbackPartitions {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
        //序列化配置
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //关联自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.echo.kafkalearning.MyPartitions");



        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);



        //发送信息
        for(int i=0;i<5;i++){
            //要指定分区，一定要检查一下当前主题有几个分区
            kafkaProducer.send(new ProducerRecord<>("first", "aldiasd" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        System.out.println("主题："+recordMetadata.topic()+"分区"+recordMetadata.partition());
                    }else{
                        System.out.println(e.getMessage());
                    }
                }
            });
        }
        //关闭资源
        kafkaProducer.close();
}}
