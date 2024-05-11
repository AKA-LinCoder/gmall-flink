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

        //acks 应答处理级别， 0 1 all=-1
        //acks=0：生产者在发送消息后不会等待服务器的确认。这意味着消息会被立即添加到生产者的发送缓冲区中，并且认为消息已经发送成功。这种配置下，生产者不会知道消息是否已经成功写入到服务器，也无法保证消息是否会丢失。
        //acks=1：生产者在发送消息后会等待领导者分区的确认。一旦消息被领导者分区成功写入到本地日志文件中，就会向生产者发送确认。在这种情况下，生产者可以确保消息至少已经被写入到领导者分区中，但是并不能保证消息是否已经被复制到其他分区或者其他副本。
        //acks=all（或者acks=-1）：生产者在发送消息后会等待所有参与复制的分区的确认。这意味着消息至少被复制到了所有的ISR（In-Sync Replicas）中的副本，并且确认已经被写入到本地日志文件中。这种配置下，生产者可以确保消息的最高可靠性，但是会降低生产者的吞吐量，因为它需要等待更多的确认。
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
