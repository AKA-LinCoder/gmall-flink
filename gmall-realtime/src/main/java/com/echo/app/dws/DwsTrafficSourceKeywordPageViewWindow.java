package com.echo.app.dws;

import com.echo.app.func.SplitFunction;
import com.echo.bean.KeywordBean;
import com.echo.utils.MyClickHouseUtil;
import com.echo.utils.MyKafkaUtil;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

//数据流 web/app 发送请求 ->nginx -> 日志服务器(.log) -> flume -> Kafka(ods) -> flinkApp -> Kafka(DWD) -> flinkApp -> clickhouse(dws)
//程序 Mock(lg.sh) -> flume -> kafka -> baseLogApp -> Kafka(Zk) ->DwsTrafficSourceKeywordPageViewWindow ->clickhouse(zk)


public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
        //TODO 获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //TODO 使用DDL 读取Kafka page_log主题的数据创建表，并且提取时间戳生成watermark
        String topic = "dwd_traffic_page_log_1";
        String groupId = "dws_traffic_source_keyword_page_view_window";
        tableEnvironment.executeSql(""+
                "create table page_log( " +
                "   `common` map<string,string>, " +
                "   `page` map<string,string>, " +
                "   `ts` bigint, " +
                "   `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                "   WATERMARK FOR rt AS rt - INTERVAL '5' SECOND"+
                ")"+ MyKafkaUtil.getKafkaDDL(topic,groupId));
        //TODO 过滤出搜索数据
        Table filterTable = tableEnvironment.sqlQuery("" +
                "select " +
                "   page['item'] item, " +
                "   rt " +
                "from page_log " +
                "where page['last_page_id'] = 'search' " +
                "and page['item_type'] = 'keyword' " +
                "and page['item'] is not null");
        tableEnvironment.createTemporaryView("filter_table",filterTable);
        //TODO 注册UDTF(一进多出) & 切词   如果用流就要用flatmap
        tableEnvironment.createTemporarySystemFunction("SplitFunction", SplitFunction.class);
        Table splitTable = tableEnvironment.sqlQuery("" +
                "SELECT " +
                " word," +
                " rt " +
                "FROM filter_table, " +
                "LATERAL TABLE(SplitFunction(item))");
        tableEnvironment.createTemporaryView("split_table",splitTable);
        //TODO 分组 开窗 聚合
        Table resultTable = tableEnvironment.sqlQuery("" +
                "select " +
                "  DATE_FORMAT(TUMBLE_START(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "  DATE_FORMAT(TUMBLE_END(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "  'search' source, " +
                "  word keyword, " +
                "  count(*) keyword_count, " +
                "  UNIX_TIMESTAMP()*1000 ts " +
                "from split_table " +
                "group by word,TUMBLE(rt,INTERVAL '10' SECOND)");
        //TODO 将动态表转换为流
        DataStream<KeywordBean> keywordBeanDataStream = null;
            keywordBeanDataStream = tableEnvironment.toDataStream(resultTable, KeywordBean.class);
            keywordBeanDataStream.print("need show data >>>>>>>>>");
            //TODO 将数据写出到clickhouse
            keywordBeanDataStream.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)"));
            //TODO 启动任务
            environment.execute("DwsTrafficSourceKeywordPageViewWindow");
    }
}
