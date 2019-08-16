package com.github.kafka.monitor.config;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import com.github.kafka.monitor.util.PropertyReader;

@Configuration
public class KafkaConfig {

	@Autowired
	PropertyReader props;

	public KafkaConsumer createConsumer(String clusterName) {
		Map<String, String> clusterProps = props.getKafkaProps().get(clusterName);
		Properties props = new Properties();
		props.put("bootstrap.servers", clusterProps.get("bootstrapServers"));
		props.put("group.id", clusterProps.get("groupId"));
		props.put("key.deserializer", clusterProps.get("keyDeserializer"));
		props.put("value.deserializer", clusterProps.get("valueDeserializer"));
		return new KafkaConsumer<byte[], byte[]>(props);
	}
	
	public KafkaAdminClient createKafkaAdminClient(String clusterName) {
		Map<String, String> clusterProps = props.getKafkaProps().get(clusterName);
		Properties props = new Properties();
		props.put("bootstrap.servers", clusterProps.get("bootstrapServers"));
		props.put("group.id", clusterProps.get("groupId"));
		props.put("key.deserializer", clusterProps.get("keyDeserializer"));
		props.put("value.deserializer", clusterProps.get("valueDeserializer"));
		return (KafkaAdminClient) KafkaAdminClient.create(props);
	}

}
