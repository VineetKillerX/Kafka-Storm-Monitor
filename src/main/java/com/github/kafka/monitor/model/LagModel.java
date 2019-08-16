package com.github.kafka.monitor.model;

import java.util.Map;

public class LagModel {
	private String topic;
	private String consumer;
	private Map<Integer,Long> offsetLag;
	private Map<Integer,Long> consumerOffset;
	private Map<Integer,Long> topicOffset;
	private long updatedAt;
	
	
	public Map<Integer, Long> getConsumerOffset() {
		return consumerOffset;
	}
	public void setConsumerOffset(Map<Integer, Long> consumerOffset) {
		this.consumerOffset = consumerOffset;
	}
	public Map<Integer, Long> getTopicOffset() {
		return topicOffset;
	}
	public void setTopicOffset(Map<Integer, Long> topicOffset) {
		this.topicOffset = topicOffset;
	}
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public String getConsumer() {
		return consumer;
	}
	public void setConsumer(String consumer) {
		this.consumer = consumer;
	}
	public Map<Integer, Long> getOffsetLag() {
		return offsetLag;
	}
	public void setOffsetLag(Map<Integer, Long> offsetLag) {
		this.offsetLag = offsetLag;
	}
	public long getUpdatedAt() {
		return updatedAt;
	}
	public void setUpdatedAt(long updatedAt) {
		this.updatedAt = updatedAt;
	}
	@Override
	public String toString() {
		return "LagModel [topic=" + topic + ", consumer=" + consumer + ", offsetLag=" + offsetLag + ", consumerOffset="
				+ consumerOffset + ", topicOffset=" + topicOffset + ", updatedAt=" + updatedAt + "]";
	}
	
	
}
