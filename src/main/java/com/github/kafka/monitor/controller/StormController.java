package com.github.kafka.monitor.controller;

import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.github.kafka.monitor.config.ApplicationCache;
import com.github.kafka.monitor.model.StormModel;
import com.github.kafka.monitor.service.StormMonitor;

@RestController
@RequestMapping(path="/storm")
@CrossOrigin(origins="*")
public class StormController {
	
	@Autowired
	StormMonitor stormMonitor;
	
	@Autowired
	ApplicationCache cache;
	
	@RequestMapping(path="/cluster",method=RequestMethod.GET,produces=MediaType.APPLICATION_JSON_VALUE)
	public Object getRunningStormClusters() {
		return stormMonitor.getAllTopologyIds(null).keySet();
		
	}
	
	@RequestMapping(path="/cluster/{clusterName}/topologies",method=RequestMethod.GET,produces=MediaType.APPLICATION_JSON_VALUE)
	public Object getClusterInfo(@PathVariable("clusterName") String clusterName) {
		return stormMonitor.getAllTopologyIds(clusterName);
		
	}
	
	
	@RequestMapping(path="/cluster/{clusterName}/topology/{id}",method=RequestMethod.GET,produces=MediaType.APPLICATION_JSON_VALUE)
	public Object getTopologyInfo(@PathVariable("clusterName") String clusterName,@PathVariable("id") String id) {
		return stormMonitor.getTopologyStat(id, clusterName);
		
	}
	
	@RequestMapping(path="/cluster/{clusterName}/topology/{id}/lag",method=RequestMethod.GET,produces=MediaType.APPLICATION_JSON_VALUE)
	public Object getTopologyLag(@PathVariable("clusterName") String clusterName,@PathVariable("id") String id) {
		StormModel model = stormMonitor.getTopologyStat(id, clusterName);
		try {
			return cache.getConsumerLag(model.getConsumerId(), model.getTopic(), model.getZkRoot());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
		
	}
	
	@RequestMapping(path="/cluster/{clusterName}/topology/{id}/offsets",method=RequestMethod.GET,produces=MediaType.APPLICATION_JSON_VALUE)
	public Object getTopologyOffsets(@PathVariable("clusterName") String clusterName,@PathVariable("id") String id) {
		StormModel model = stormMonitor.getTopologyStat(id, clusterName);
		try {
			return cache.getConsumerOffsets(model.getConsumerId(), model.getZkRoot());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
		
	}
}
