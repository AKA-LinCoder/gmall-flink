package com.echo.kafkalearning;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义拦截器
 */
public class ValueInterceptor implements ProducerInterceptor<String,String> {
    @Override
    /**
     * 发送数据时调用
     */
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return new ProducerRecord<>(producerRecord.topic(), producerRecord.key(), producerRecord.value()+"2");
    }

    /**
     * 发送数据完毕，服务器返回的响应，会调用此方法
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    /**
     * 生产者对象关闭的时候会调用此方法
     */
    @Override
    public void close() {

    }

    /**
     * 创建生产者对应的时候调用
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {

    }
}
