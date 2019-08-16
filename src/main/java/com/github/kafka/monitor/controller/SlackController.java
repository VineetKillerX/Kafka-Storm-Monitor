package com.github.kafka.monitor.controller;

import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.github.kafka.monitor.service.SlackService;

@RestController
@RequestMapping(path="/slack")
public class SlackController {
	
	@Autowired
	SlackService slackService;
	
	@RequestMapping(path="/storm/lag",method=RequestMethod.GET,produces=MediaType.APPLICATION_JSON_VALUE)
	public Object getTopologylag(@RequestParam String topologyId) {
		try {
			return slackService.getStormTopologyLag(topologyId);
		} catch (ExecutionException e) {
			e.printStackTrace();
			return e;
		}
	}
	
	@RequestMapping(path="/storm/topologies",method=RequestMethod.GET,produces=MediaType.APPLICATION_JSON_VALUE)
	public Object getListOfTopologies() {
		try {
			return slackService.getListOfTopologies();
		} catch (ExecutionException e) {
			e.printStackTrace();
			return e;
		}
	}
	
	@RequestMapping(path="/storm/offset",method=RequestMethod.GET,produces=MediaType.APPLICATION_JSON_VALUE)
	public Object getTopologyOffsets(@RequestParam String topologyId) {
		try {
			return slackService.getStormTopologyLag(topologyId);
		} catch (ExecutionException e) {
			e.printStackTrace();
			return e;
		}
	}
	
	@RequestMapping(path="/kafka/topic",method=RequestMethod.GET,produces=MediaType.APPLICATION_JSON_VALUE)
	public Object getTopicOffsets(@RequestParam String topicName) {
		try {
			return slackService.getKafkaTopicOffsets(topicName);
		} catch (ExecutionException e) {
			e.printStackTrace();
			return e;
		}
	}
}
