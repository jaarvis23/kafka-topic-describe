package com.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Node;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class KafkaTopicDescribe {

    public static void main(String[] args) {
        // Set up Kafka properties
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Create an AdminClient
        try (AdminClient adminClient = AdminClient.create(properties)) {

            // Call the method for -> kafka-topics.sh --topic {topicName} --describe
            DescribeTopic(adminClient);

            // Call the method for -> kafka-topics.sh --list
            ListTopics(adminClient);

            // Call the method for -> kafka-console-consumer.sh --topic {topicName} --from-beginning
            executeConsumer();


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Method to execute the command -> kafka-console-consumer.sh --topic {topicName} --from-beginning
    private static void DescribeTopic(AdminClient adminClient) {
        // Specify the topic to describe
        String topicName = "test-topic";

        // Describe the specified topic
        DescribeTopicsResult describeResult = adminClient.describeTopics(Collections.singletonList(topicName));

        // Get the result for the specified topic
        Map<String, KafkaFuture<TopicDescription>> futures = describeResult.values();

        // Get the TopicDescription for the specified topic
        KafkaFuture<TopicDescription> future = futures.get(topicName);

        // Handle the result when it is available
        try {
            TopicDescription topicDescription = future.get();
            // Format and print the topic information
            System.out.println("Topic: " + topicDescription.name());
            System.out.println("        TopicId: " + topicDescription.topicId());
            System.out.println("        PartitionCount: " + topicDescription.partitions().size());
            System.out.println("        ReplicationFactor: " + topicDescription.partitions().get(0).replicas().size());
            System.out.println("Configs: ");

            // Print information about each partition
            for (TopicPartitionInfo partitionInfo : topicDescription.partitions()) {
                System.out.println("        Partition: " + partitionInfo.partition());
                System.out.println("        Leader: " + partitionInfo.leader().id());
                System.out.println("        Replicas: " + partitionInfo.replicas().stream().map(Node::id).collect(Collectors.toList()));
                System.out.println("        Isr: " + partitionInfo.isr().stream().map(Node::id).collect(Collectors.toList()));
                System.out.println();
            }

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    //Method to execute the command -> kafka-topics.sh --topic {topicName} --describe
    
    private static void ListTopics(AdminClient adminClient){
        try {
            Collection<String> topics = adminClient.listTopics().names().get();
            System.out.println("List of Topics:");
            for (String topic : topics) {
                System.out.println(topic);
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    // Method to execute the kafka-console-consumer.sh command
    private static void executeConsumer() {
        // Set Kafka broker address
        String bootstrapServers = "localhost:9092";

        // Set the consumer group id
        String groupId = "test";

        // Set the topic to subscribe to
        String topic = "test-topic";

        // Create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create Kafka consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to the Kafka topic
        consumer.subscribe(Collections.singletonList(topic));

        // poll(listen) for records and print messages
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> {
                    System.out.println("Received message: " + record.value());
                });
            }
        } finally {
            // Close the consumer
            consumer.close();
        }
    }

}
