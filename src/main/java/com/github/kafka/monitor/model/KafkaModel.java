package com.github.kafka.monitor.model;

import java.util.List;

public class KafkaModel {
	private String clusterId;
	private List<String> nodes;
	public String getClusterId() {
		return clusterId;
	}
	public void setClusterId(String clusterId) {
		this.clusterId = clusterId;
	}
	public List<String> getNodes() {
		return nodes;
	}
	public void setNodes(List<String> nodes) {
		this.nodes = nodes;
	}
	@Override
	public String toString() {
		return "KafkaModel [clusterId=" + clusterId + ", nodes=" + nodes + "]";
	}
	
	
	
}
