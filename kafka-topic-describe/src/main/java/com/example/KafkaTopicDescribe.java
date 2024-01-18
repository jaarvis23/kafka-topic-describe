package com.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Node;

import java.io.BufferedReader;
import java.io.InputStreamReader;
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

    // Method to format and print the contents of Topic Describe Command
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
        try {
            String command = "/root/kafka-tar/kafka_2.13-3.5.1/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning";
            Process process = Runtime.getRuntime().exec(command);

            // Read the output of the process
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

            // Wait for the process to complete
            int exitCode = process.waitFor();
            System.out.println("Command exited with code: " + exitCode);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
