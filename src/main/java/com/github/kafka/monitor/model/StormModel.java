package com.github.kafka.monitor.model;

import java.util.Map;

public class StormModel {
	private String consumerId;
	private String topic;
	private Map<Integer,Long> offsetMap;
	private String topologyId;
	private String zkRoot;
	private String brokers;
	
	
	public String getBrokers() {
		return brokers;
	}
	public void setBrokers(String brokers) {
		this.brokers = brokers;
	}
	public String getZkRoot() {
		return zkRoot;
	}
	public void setZkRoot(String zkRoot) {
		this.zkRoot = zkRoot;
	}
	@Override
	public String toString() {
		return "StormModel [consumerId=" + consumerId + ", topic=" + topic + ", offsetMap=" + offsetMap
				+ ", topologyId=" + topologyId + ", zkRoot=" + zkRoot + ", brokers=" + brokers + "]";
	}
	public String getConsumerId() {
		return consumerId;
	}
	public void setConsumerId(String consumerId) {
		this.consumerId = consumerId;
	}
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public Map<Integer, Long> getOffsetMap() {
		return offsetMap;
	}
	public void setOffsetMap(Map<Integer, Long> offsetMap) {
		this.offsetMap = offsetMap;
	}
	public String getTopologyId() {
		return topologyId;
	}
	public void setTopologyId(String topologyId) {
		this.topologyId = topologyId;
	}
	
	
	
}
