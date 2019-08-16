package com.github.kafka.monitor.controller;

import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.github.kafka.monitor.config.ApplicationCache;
import com.github.kafka.monitor.service.KafkaConsumerMonitor;
import com.github.kafka.monitor.service.StormMonitor;
import com.github.kafka.monitor.service.ZookeeperMonitor;

@RestController
@RequestMapping(path="/kafka")
@CrossOrigin(origins="*")
public class KafkaMonitorController {
	
	@Autowired
	KafkaConsumerMonitor kafkaMonitor;
	
	@Autowired
	ZookeeperMonitor zooMonitor;
	
	@Autowired
	StormMonitor stormMonitor;
	
	@Autowired
	ApplicationCache cache;
	
	Logger logger = LoggerFactory.getLogger(KafkaMonitorController.class);

	@RequestMapping(path="/clusters",method=RequestMethod.GET,produces=MediaType.APPLICATION_JSON_VALUE)
	public Object getKafkaCluster() {
		return cache.getListOfTopics().keySet();
	}
	
	@RequestMapping(path="/clusters/{clusterName}",method=RequestMethod.GET,produces=MediaType.APPLICATION_JSON_VALUE)
	public Object getKafkaCluster(@PathVariable("clusterName") String clusterName) throws InterruptedException, ExecutionException {
		return kafkaMonitor.getClusterInfo(clusterName);
	}
	
	@RequestMapping(path="/clusters/{clusterName}/topics",method=RequestMethod.GET,produces=MediaType.APPLICATION_JSON_VALUE)
	public Object getTopicsForKafkaCluster(@PathVariable("clusterName") String clusterName) {
		return cache.getListOfTopics().get(clusterName);
	}
	
	@RequestMapping(path="/clusters/{clusterName}/topics/{topicName}",method=RequestMethod.GET,produces=MediaType.APPLICATION_JSON_VALUE)
	public Object getTopicsList(@PathVariable("clusterName") String clusterName,@PathVariable("topicName") String topicName) {
		try {
			return cache.getFromTopicOffsetCache(topicName, null);
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	@RequestMapping(path="/cache",method=RequestMethod.GET,produces=MediaType.APPLICATION_JSON_VALUE)
	public Object getCacheDetails() {
		return cache.getCacheDetails();
	}
	
	@RequestMapping(path="/clusters/{clusterName}/consumers",method=RequestMethod.GET,produces=MediaType.APPLICATION_JSON_VALUE)
	public Object getKafkaConsumers(@PathVariable("clusterName") String clusterName) {
		try {
			return kafkaMonitor.getConsumers(clusterName);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return "NOT FOUND";
	}
	
	
	@RequestMapping(path="/clusters/{clusterName}/consumers/{consumerId}",method=RequestMethod.GET,produces=MediaType.APPLICATION_JSON_VALUE)
	public Object readConsumer(@PathVariable("clusterName") String clusterName,@PathVariable("consumerId") String consumerId) {
		kafkaMonitor.readConsumer(clusterName);
		return "OK";
	}
}
