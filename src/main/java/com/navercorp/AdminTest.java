package com.navercorp;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminTest {
	private static Logger LOGGER = LoggerFactory.getLogger("KafkaLogger");

	public static void main(String[] args) {
		KafkaAdmin kafkaAdmin = new KafkaAdmin(args[0]);
		try {
			// Cluster ID
			String clusterId = kafkaAdmin.getClusterId();
			LOGGER.info("Cluster ID : {}", clusterId);

			// Topic List
			Set<String> topicNameSet = kafkaAdmin.getTopicNameSet();
			LOGGER.info("Topic List ---------------------------------------------------");
			topicNameSet.forEach((topicName) ->
					LOGGER.info("{}", topicName)
			);
			LOGGER.info("--------------------------------------------------------------");

			// Topic Detail
			topicNameSet.forEach((topicName) -> {
				TopicDescription topic = kafkaAdmin.getTopicDetail(topicName);

				LOGGER.info("Topic --------------------------------------------------------");
				LOGGER.info("name : {}", topic.name());
				LOGGER.info("internal : {}", topic.isInternal());
				LOGGER.info("partitions :");
				topic.partitions().forEach((partition) -> {
					StringBuilder replicasBuilder = new StringBuilder();
					replicasBuilder.append("[");
					partition.replicas().forEach((replica) -> {
						replicasBuilder.append(replica.id());
						replicasBuilder.append(",");
					});
					replicasBuilder.append("]");
					String replicas = replicasBuilder.toString();
					StringBuilder isrBuilder = new StringBuilder();
					isrBuilder.append("[");
					partition.isr().forEach((isr) -> {
						isrBuilder.append(isr.id());
						isrBuilder.append(",");
					});
					isrBuilder.append("]");
					String isr = isrBuilder.toString();

					LOGGER.info("    id={}, leader={}, replicas={}, isr={}",
							partition.partition(),
							partition.leader().id(),
							replicas,
							isr
					);
				});
				LOGGER.info("--------------------------------------------------------------");
			});
		} finally {
			kafkaAdmin.close();
		}
	}

	private static class KafkaAdmin {
		private AdminClient adminClient;

		KafkaAdmin(String servers) {
			Properties properties = new Properties();
			properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servers);

			this.adminClient = AdminClient.create(properties);
		}

		Set<String> getTopicNameSet() {
			ListTopicsResult listTopicsResult = adminClient.listTopics();

			try {
				return listTopicsResult.names().get();
			} catch (ExecutionException e) {
				LOGGER.error("getTopicNameSet ExecutionException", e);
			} catch (InterruptedException e) {
				LOGGER.error("Interrupted", e);
			}

			return Collections.emptySet();
		}

		String getClusterId() {
			DescribeClusterResult result = adminClient.describeCluster();

			try {
				return result.clusterId().get();
			} catch (ExecutionException e) {
				LOGGER.error("getClusterId ExecutionException", e);
			} catch (InterruptedException e) {
				LOGGER.error("Interrupted", e);
			}

			return "";
		}

		TopicDescription getTopicDetail(String topicName) {
			DescribeTopicsResult result = adminClient.describeTopics(Arrays.asList(topicName));

			try {
				return result.values().get(topicName).get();
			} catch (ExecutionException e) {
				LOGGER.error("getClusterId ExecutionException", e);
			} catch (InterruptedException e) {
				LOGGER.error("Interrupted", e);
			}

			return null;
		}

		void close() {
			adminClient.close();
		}
	}
}
