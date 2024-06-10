package com.filkod.spring.kafka.leaderlatch.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Configuration
public class KafkaTopicConfig {
    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${zookeeper.connect-string}")
    private String zookeeperConnectString;

    @Value("${spring.kafka.group.id}")
    private String groupId;

    @Bean
    public KafkaConsumer<String, String> kafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return new KafkaConsumer<>(props);
    }

    @Bean
    public CuratorFramework curatorFramework() {
        int sessionTimeoutMs = 10000; // 10 seconds
        int connectionTimeoutMs = 3000; // 3 seconds
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                zookeeperConnectString, sessionTimeoutMs, connectionTimeoutMs, new ExponentialBackoffRetry(1000, 3));
        client.start();
        return client;
    }

    @Bean
    public LeaderLatch leaderLatch(CuratorFramework client) throws Exception {
        LeaderLatch leaderLatch = new LeaderLatch(client, "/leader-election");
        leaderLatch.start();
        return leaderLatch;
    }

    @Bean
    public AdminClient adminClient() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(configs);
    }

    @Bean
    public NewTopic createTopic() {
        return new NewTopic("example-topic", 2, (short) 1);
    }
}
