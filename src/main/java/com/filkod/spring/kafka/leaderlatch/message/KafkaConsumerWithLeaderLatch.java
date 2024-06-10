package com.filkod.spring.kafka.leaderlatch.message;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Collections;

@Component
public class KafkaConsumerWithLeaderLatch extends LeaderSelectorListenerAdapter {

    @Value("${spring.kafka.topic.name}")
    private String topicName;

    @Autowired
    private KafkaConsumer<String, String> kafkaConsumer;

    @Autowired
    private CuratorFramework curatorFramework;

    private LeaderSelector leaderSelector;
    private static final int MAX_MESSAGES_BEFORE_RELINQUISH = 5;
    private static final long REJOIN_LEADER_DELAY_MS = 6000 * 1000; // 6000 seconds

    @PostConstruct
    public void init() {
        // Initialize the leader selector
        initializeLeaderSelector();
    }

    private void initializeLeaderSelector() {
        leaderSelector = new LeaderSelector(curatorFramework, "/leader-election", this);
        leaderSelector.autoRequeue(); // Automatically requeue after relinquishing leadership
        leaderSelector.start();
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        System.out.println("Became leader at: " + System.currentTimeMillis());
        kafkaConsumer.subscribe(Collections.singletonList(topicName));
        int messageCount = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumed message: (%s, %s) from partition: %d%n",
                            record.key(), record.value(), record.partition());
                    messageCount++;
                    if (messageCount >= MAX_MESSAGES_BEFORE_RELINQUISH) {
                        System.out.println("Relinquishing leadership after consuming " + MAX_MESSAGES_BEFORE_RELINQUISH + " messages.");
                        return; // Relinquish leadership by returning from takeLeadership method
                    }
                }
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
        } finally {
            kafkaConsumer.close();
        }
    }

    private void rejoinLeadershipAfterDelay() {
        new Thread(() -> {
            try {
                System.out.println("Waiting for " + REJOIN_LEADER_DELAY_MS / 1000 + " seconds before rejoining leadership...");
                Thread.sleep(REJOIN_LEADER_DELAY_MS);
                System.out.println("Attempting to rejoin leadership...");
                initializeLeaderSelector();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
    }

    @PreDestroy
    public void destroy() {
        try {
            if (leaderSelector != null) {
                leaderSelector.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
