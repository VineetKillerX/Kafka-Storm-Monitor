package com.github.kafka.monitor.service;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.github.kafka.monitor.config.KafkaConfig;
import com.github.kafka.monitor.model.KafkaModel;
import com.github.kafka.monitor.model.TopicModel;
import com.github.kafka.monitor.util.PropertyReader;

import kafka.coordinator.group.GroupMetadataManager;
import kafka.coordinator.group.OffsetKey;

@Service
public class KafkaConsumerMonitor {

	@Autowired
	KafkaConfig kafkaConfig;

	@Autowired
	PropertyReader properties;

	public KafkaModel getClusterInfo(String clusterName) throws InterruptedException, ExecutionException {
		KafkaAdminClient kafkaAdminClient = kafkaConfig.createKafkaAdminClient(clusterName);
		DescribeClusterResult result = kafkaAdminClient.describeCluster();
		KafkaModel kafkaModel = new KafkaModel();
		KafkaFuture<String> clusterFuture = result.clusterId();
		KafkaFuture<Collection<Node>> clusterNodesFuture = result.nodes();
		kafkaModel.setClusterId(clusterFuture.get());
		List<String> nodes = clusterNodesFuture.get().stream().map(e -> e.host() + ":" + e.port())
				.collect(Collectors.toList());
		kafkaModel.setNodes(nodes);
		return kafkaModel;

	}

	public List<String> getConsumers(String clusterName) throws InterruptedException, ExecutionException {
		if (clusterName != null) {
			KafkaAdminClient client = kafkaConfig.createKafkaAdminClient(clusterName);
			ListConsumerGroupsResult consumers = client.listConsumerGroups();
			KafkaFuture<Collection<ConsumerGroupListing>> result = consumers.all();
			return result.get().stream()
					.filter(e -> !e.groupId().contains("console") || e.groupId().contains("test-consumer"))
					.map(e -> e.groupId()).collect(Collectors.toList());
		} else {
			return null;
		}

	}

	public Map<String, List<String>> getTopics() {
		Map<String, List<String>> clusterTopicsMap = new HashMap<>();
		for (String string : properties.getKafkaProps().keySet()) {
			List<String> topicsList = new ArrayList<>();
			KafkaConsumer<byte[], byte[]> kafkaConsumer = kafkaConfig.createConsumer(string);
			Map<String, List<PartitionInfo>> topicsMap = kafkaConsumer.listTopics();
			kafkaConsumer.close(Duration.ofSeconds(1));
			topicsList.addAll(topicsMap.keySet());
			clusterTopicsMap.put(string, topicsList);
		}
		return clusterTopicsMap;
	}

	public Map<String, Map<String, Integer>> getTopicsWithPartitions(String clusterName) {
		Map<String, Map<String, Integer>> clusterTopicsMap = new HashMap<>();
		Map<String, Integer> topicsList = new HashMap<>();
		if (clusterName != null && !clusterName.isEmpty()) {
			KafkaConsumer<byte[], byte[]> kafkaConsumer = kafkaConfig.createConsumer(clusterName);
			Map<String, List<PartitionInfo>> topicsMap = kafkaConsumer.listTopics();
			kafkaConsumer.close(Duration.ofSeconds(1));
			for (Map.Entry<String, List<PartitionInfo>> entry : topicsMap.entrySet()) {
				topicsList.put(entry.getKey(), entry.getValue().size());
			}
			clusterTopicsMap.put(clusterName, topicsList);
		} else {
			for (String string : properties.getKafkaProps().keySet()) {
				KafkaConsumer<byte[], byte[]> kafkaConsumer = kafkaConfig.createConsumer(string);
				Map<String, List<PartitionInfo>> topicsMap = kafkaConsumer.listTopics();
				kafkaConsumer.close(Duration.ofSeconds(1));
				for (Map.Entry<String, List<PartitionInfo>> entry : topicsMap.entrySet()) {
					topicsList.put(entry.getKey(), entry.getValue().size());
				}
				clusterTopicsMap.put(string, topicsList);
			}
		}

		return clusterTopicsMap;
	}

	public Integer getPartitionsOfTopic(String topic) {
		for (String string : properties.getKafkaProps().keySet()) {
			KafkaConsumer<byte[], byte[]> kafkaConsumer = kafkaConfig.createConsumer(string);
			Map<String, List<PartitionInfo>> topicsMap = kafkaConsumer.listTopics();
			kafkaConsumer.close(Duration.ofSeconds(1));
			if (topicsMap.containsKey(topic)) {
				return topicsMap.get(topic).size();
			}
		}
		return null;
	}

	public TopicModel getTopicDetails(String topic, int numberOfPartitions) {
		for (String string : properties.getKafkaProps().keySet()) {
			KafkaConsumer<byte[], byte[]> kafkaConsumer = kafkaConfig.createConsumer(string);
			TopicModel model = new TopicModel();
			Map<Integer, Long> partitionMap = new HashMap<>(numberOfPartitions);
			List<TopicPartition> partitions = new ArrayList<>();
			for (int i = 0; i < numberOfPartitions; i++) {
				TopicPartition topicPartition = new TopicPartition(topic, i);
				partitions.add(topicPartition);
			}
			Map<TopicPartition, Long> offsetMap = kafkaConsumer.endOffsets(partitions);
			kafkaConsumer.close(Duration.ofSeconds(1));
			for (Map.Entry<TopicPartition, Long> entry : offsetMap.entrySet()) {
				partitionMap.put(entry.getKey().partition(), entry.getValue());
			}
			model.setPartitionsMap(partitionMap);
			model.setName(topic);
			if (!partitionMap.isEmpty()) {
				return model;
			}
		}
		return null;
	}

	public List<TopicModel> getAllTopicsDetails(String clusterName) {
		List<TopicModel> topicDetails = new ArrayList<>();
		Map<String, Map<String, Integer>> map = getTopicsWithPartitions(clusterName);
		for (Entry<String, Map<String, Integer>> entry : map.entrySet()) {
			clusterName = entry.getKey();
			for (Entry<String, Integer> entryMap : entry.getValue().entrySet()) {
				TopicModel model = getTopicDetails(entryMap.getKey(), entryMap.getValue());
				model.setClusterName(clusterName);
				topicDetails.add(model);
			}
		}
		return topicDetails;
	}

	public void readConsumer(String clusterName) {
		KafkaConsumer<byte[], byte[]> kafkaConsumer = kafkaConfig.createConsumer(clusterName);
		kafkaConsumer.subscribe(Collections.singletonList("__consumer_offsets"));
		ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(10));
		Iterator<ConsumerRecord<byte[], byte[]>> itr = consumerRecords.iterator();
		while (itr.hasNext()) {
			byte[] key = itr.next().key();
			if (key != null) {
				Object o = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key));
				if (o != null) {
					OffsetKey offsetKey = (OffsetKey) o;
					Object groupTopicPartition = offsetKey.key();
					System.out.println(groupTopicPartition.toString());
				}
			}
		}
		kafkaConsumer.close(Duration.ofSeconds(1));
	}

}
