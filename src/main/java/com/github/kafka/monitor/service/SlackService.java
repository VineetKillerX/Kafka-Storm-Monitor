package com.github.kafka.monitor.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.github.kafka.monitor.config.ApplicationCache;
import com.github.kafka.monitor.model.SlackLagModel;
import com.github.kafka.monitor.model.StormModel;
import com.github.kafka.monitor.model.TopicModel;

@Service
public class SlackService {
	
	@Autowired
	ApplicationCache cache;
	
	@Autowired
	StormMonitor stormService;
	
	@Autowired
	KafkaConsumerMonitor kafkaMonitor;
	
	@Autowired
	ZookeeperMonitor zookeeperMonitor;
	
	public Object getStormTopologyLag(String topologyId) throws ExecutionException {
		Map<String,List<String>> stormclusterMap  = stormService.getAllTopologyIds(null);
		String clusterName = null;
		for (Map.Entry<String, List<String>> entries : stormclusterMap.entrySet()) {
			for (String listName : entries.getValue()){
				if(listName.contains(topologyId)) {
					clusterName = entries.getKey();
					topologyId = listName;
					break;
				}
			}
            for (String listName : entries.getValue()) {
                if (listName.equalsIgnoreCase(topologyId)) {
                    topologyId = listName;
                    break;
                }
            }
		}
		if(clusterName!=null) {
			StormModel model = stormService.getTopologyStat(topologyId, clusterName);
			if(model!=null) {
				Map<Integer,Long> lagMap = new HashMap<>();
				int partitions = kafkaMonitor.getPartitionsOfTopic(model.getTopic());
				TopicModel topicModel =  kafkaMonitor.getTopicDetails(model.getTopic(), partitions);
				model = zookeeperMonitor.getZookeeperData(model);
				if(topicModel.getPartitionsMap()!=null && model.getOffsetMap()!=null) {
					for (int i = 0; i < partitions; i++) {
						long lag = topicModel.getPartitionsMap().get(i) - model.getOffsetMap().get(i);
						lagMap.put(i, lag);
					}
				}
				SlackLagModel slackLagModel = new SlackLagModel(topologyId, lagMap);
				return slackLagModel;
			}else {
				return "Topology Information not found";
			}
		}else {
			return "Topology ID not found";
		}
	}
	
	public Object getStormTopologyOffsets(String topologyId) throws ExecutionException {
		Map<String,List<String>> stormclusterMap  = stormService.getAllTopologyIds(null);
		String clusterName = null;
		for (Map.Entry<String, List<String>> entries : stormclusterMap.entrySet()) {
			for (String listName : entries.getValue()){
				if(listName.contains(topologyId)) {
					clusterName = entries.getKey();
					topologyId = listName;
					break;
				}
			}
            for (String listName : entries.getValue()) {
                if (listName.equalsIgnoreCase(topologyId)) {
                    topologyId = listName;
                    break;
                }
            }
		}
		if(clusterName!=null) {
			StormModel model = stormService.getTopologyStat(topologyId, clusterName);
			if(model!=null) {
				model = zookeeperMonitor.getZookeeperData(model);
				SlackLagModel slackLagModel = new SlackLagModel(topologyId, model.getOffsetMap());
				return slackLagModel;
			}else {
				return "Topology Information not found";
			}
		}else {
			return "Topology ID not found";
		}
	}
	
	public Object getKafkaTopicOffsets(String topic) throws ExecutionException {
		return cache.getFromTopicOffsetCache(topic, null);
	}
	
	public Object getListOfTopologies() throws ExecutionException {
		return cache.getTopologyConsumerInfo().stream().map(e->e.getTopologyId()).collect(Collectors.toList());
	}
}
