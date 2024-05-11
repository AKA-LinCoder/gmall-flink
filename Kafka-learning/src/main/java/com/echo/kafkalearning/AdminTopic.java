package com.echo.kafkalearning;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AdminTopic {
    public static void main(String[] args) {

        //TODO 使用Admin创建主题
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        Admin admin = Admin.create(properties);
        short nn = 1;


        Map<Integer, java.util.List<Integer>> replicasAssignments = new HashMap<>();
        //TODO 0号分区有两个副本，把第一个副本放在3分区中，第二个副本放在1分区中，写在前面的就是leader
        replicasAssignments.put(0,Arrays.asList(3,1));
        replicasAssignments.put(1,Arrays.asList(1,2));
        replicasAssignments.put(2,Arrays.asList(1 ,3));
        // In - Sync -Replicas :同步副本列表(ISR)
        NewTopic newTopic1 = new NewTopic("topic1111",replicasAssignments);
        NewTopic newTopic = new NewTopic("dasda",1,nn);
        CreateTopicsResult topics = admin.createTopics(Arrays.asList(newTopic,newTopic1));
        admin.close();
    }
}
