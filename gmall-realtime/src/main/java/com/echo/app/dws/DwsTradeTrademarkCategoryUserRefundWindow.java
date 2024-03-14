package com.echo.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.echo.bean.TradeTrademarkCategoryUserRefundBean;
import com.echo.utils.DateFormatUtil;
import com.echo.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class DwsTradeTrademarkCategoryUserRefundWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        String topic = "dwd_trade_order_refund";
        String groupId = "dws_trade_trademark_category_user_refund_window";
        DataStreamSource<String> kafkaDS = environment.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 每行数据转化为javabean
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tradeTrademarkCategoryUserRefundBeanDS = kafkaDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            HashSet<String> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getString("order_id"));
            return TradeTrademarkCategoryUserRefundBean.builder()
                    .skuId(jsonObject.getString("sku_id"))
                    .userId(jsonObject.getString("user_id"))
                    .orderIdSet(orderIds)
                    .ts(DateFormatUtil.toTs(jsonObject.getString("create_time"), true))
                    .build();
        });
        //TODO 关联sku_info维表补充tm_id以及category3_id
//        AsyncDataStream.unorderedWait(tradeTrademarkCategoryUserRefundBeanDS,new DimAsyncFunction)
        //TODO 分组开窗聚合
        //TODO 关联维表补充其他字段
        //TODO 写入clickhouse


        environment.execute("DwsTradeTrademarkCategoryUserRefundWindow");
    }
}
