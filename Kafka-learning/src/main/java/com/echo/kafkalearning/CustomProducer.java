package com.echo.kafkalearning;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
        //序列化配置
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        //TODO 使用自定义分区
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,MyPartitions.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //发送信息
        for(int i=0;i<5;i++){
            kafkaProducer.send(new ProducerRecord<>("first","linguanyu"+i));
        }
        //关闭资源
        kafkaProducer.close();
    }
}