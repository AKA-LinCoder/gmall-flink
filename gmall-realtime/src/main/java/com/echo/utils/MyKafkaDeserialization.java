package com.echo.utils;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class MyKafkaDeserialization implements KafkaDeserializationSchema<String> {

    @Override
    public boolean isEndOfStream(String s) {
        return false;
    }

    @Override
    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        if(consumerRecord==null || consumerRecord.value() == null){
            return null;
        }else {
            return new String(consumerRecord.value());
        }
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
