package com.echo.kafkalearning;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerAcks {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //连接Kafka集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        //序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        /// 下面是提高生产者吞吐量的几个参数

        //缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        //批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);

        //linger.ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);

        //压缩
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

        //acks
        properties.put(ProducerConfig.ACKS_CONFIG,"1");

        //重试次数,默认是int的最大值
        properties.put(ProducerConfig.RETRIES_CONFIG,2);




        //创建生产者
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        //发送数据

        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("first","dasdasda"));
        }
        //关闭资源
        kafkaProducer.close();
    }
}
