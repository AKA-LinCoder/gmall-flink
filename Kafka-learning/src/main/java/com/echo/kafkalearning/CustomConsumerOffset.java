package com.echo.kafkalearning;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;

public class CustomConsumerOffset {
    public static void main(String[] args) {
        // 创建一个消费者
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //TODO 零拷贝：Kafka传递数据时并不将数据从操作系统读取到Kafka然后再层层传递给用户，而是直接通过fileChannel。transforTO发指令给操作系统通过特权指令直接发送

        //LEO :默认 size+1
        //设置偏移，默认latest，就只会读取新增的，如果改成earliest，就会从头读取
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        //配置消费者组ID
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");

        //TODO 关闭自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);

        //TODO 事务隔离级别: read_committed，read_uncommitted
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        // 订阅主题
        ArrayList<String> topics = new ArrayList<>();
        topics.add("first");
        kafkaConsumer.subscribe(topics);
        //获取分区
        Set<TopicPartition> assignment = kafkaConsumer.assignment();

        //TODO 从指定位置开始消费
        boolean flg = true;
        while (flg){
            kafkaConsumer.poll(Duration.ofMillis(100));
            Set<TopicPartition> assignment1 = kafkaConsumer.assignment();
            if(assignment1!=null && !assignment1.isEmpty()){
                for (TopicPartition topicPartition : assignment1) {
                    kafkaConsumer.seek(topicPartition,2);
                    flg = false;
                }
            }
        }



        //保证分区分配方案以及指定完毕
        while (assignment.size() == 0){
            kafkaConsumer.poll(Duration.ofSeconds(1));
            assignment = kafkaConsumer.assignment();
        }

        for (TopicPartition topicPartition : assignment) {
            System.out.println(topicPartition.partition());
        }


        // 消费数据吧
        while (true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println(record);
            }
            //TODO 如果关闭了自动保存偏移量，就必须手动保存偏移量
            kafkaConsumer.commitAsync();
        }

    }
}
