package com.github.kafka.monitor.model;

import java.util.Map;

public class TopicModel {
	private String name;
	private long partitions;
	private Map<Integer,Long> partitionsMap;
	private String clusterName;
	
	
	public String getClusterName() {
		return clusterName;
	}
	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}
	public long getPartitions() {
		return partitions;
	}
	public void setPartitions(long partitions) {
		this.partitions = partitions;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Map<Integer, Long> getPartitionsMap() {
		return partitionsMap;
	}
	public void setPartitionsMap(Map<Integer, Long> partitionsMap) {
		this.partitionsMap = partitionsMap;
	}
	@Override
	public String toString() {
		return "TopicModel [name=" + name + ", partitions=" + partitions + ", partitionsMap=" + partitionsMap
				+ ", clusterName=" + clusterName + "]";
	}
	public TopicModel(String name, Map<Integer, Long> partitionsMap, long partitions) {
		super();
		this.name = name;
		this.partitionsMap = partitionsMap;
		this.partitions = partitions;
	}
	
	public TopicModel() {
		
	}
	
	
	
	
}
