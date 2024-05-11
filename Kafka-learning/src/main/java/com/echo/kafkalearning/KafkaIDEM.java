package com.echo.kafkalearning;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaIDEM {


    public static void main(String[] args) {
        Properties properties = new Properties();
        //TODO 设置幂等性,默认为false，幂等性操作只能保证一个分区内的数据不重复，跨会话的幂等由事务来保证
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);
        //固定为-1
        properties.put(ProducerConfig.ACKS_CONFIG,"-1");
        //开启重试
        properties.put(ProducerConfig.RETRIES_CONFIG,5);
        //TODO 事务设置,事务解决的是跨会话的幂等性问题，不能解决跨分区的幂等性问题
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"my-tx-id");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        //TODO 初始化事务
        kafkaProducer.initTransactions();
        try {
            //TODO 开启事务
            kafkaProducer.beginTransaction();
            kafkaProducer.send(new ProducerRecord<>("test","123"));
            //TODO 提交事务
            kafkaProducer.commitTransaction();
        }catch (Exception e){
            //TODO 终止事务
            kafkaProducer.abortTransaction();
        }finally {
            kafkaProducer.close();
        }

        //TODO Kafka拿到数据并不会直接写入文件，因为kafka认为刚写的数据会马上被用到，所有会放入到内存中，只有数据到达一定量时才会写入文件






    }
}
