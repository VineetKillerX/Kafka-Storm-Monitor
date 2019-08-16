package com.github.kafka.monitor.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.github.kafka.monitor.model.StormModel;
import com.github.kafka.monitor.util.PropertyReader;

@Service
public class StormMonitor {

	@Autowired
	ZookeeperMonitor zooMonitor;

	@Autowired
	PropertyReader reader;

	Map<String, String> stormClusterMap = new HashMap<>();

	public List<String> topologiesId = new ArrayList<>();
	public List<StormModel> topologyStats = new ArrayList<>();

	public static final String STORM_UI_URL = "http://storm1.abc:8080";
	public static final String STORM_PREFIX = "/api/v1/topology";
	public static final String SUMMARY = "/summary";

	private void init() {
		if (stormClusterMap.isEmpty()) {
			for (Map.Entry<String, Map<String, String>> entry : reader.getStormProps().entrySet()) {
				stormClusterMap.put(entry.getKey(), entry.getValue().get("stormUrl"));
			}
		}

	}

	public Map<String, List<String>> getAllTopologyIds(String clusterName) {
		init();
		HttpClient client = HttpClientBuilder.create().build();
		List<String> blackListedTopologies = new ArrayList<>();
		List<String> blackListedTopologiesId = new ArrayList<>();
		Map<String, List<String>> topologyMap = new HashMap<>();
		List<String> topologiesId = new ArrayList<>();
		String content = null;
		HttpGet summaryGet = null;
		if (clusterName != null) {
			summaryGet = new HttpGet(stormClusterMap.get(clusterName) + STORM_PREFIX + SUMMARY);
			try {
				HttpResponse response = client.execute(summaryGet);
				HttpEntity respEntity = response.getEntity();
				if (respEntity != null) {
					content = EntityUtils.toString(respEntity);
				}
				JSONObject json = new JSONObject(content);
				JSONArray jsArr = json.getJSONArray("topologies");
				Object obj = reader.getStormProps().get(clusterName).get("blackListedTopologies");
				if (obj != null) {
					blackListedTopologies = Arrays.asList(String.valueOf(obj).split(","));
				}
				for (int i = 0; i < jsArr.length(); i++) {
					JSONObject topologyJson = jsArr.getJSONObject(i);
					topologiesId.add(topologyJson.getString("id").toString());
					for (String string : blackListedTopologies) {
						if (topologyJson.getString("id").contains(string)) {
							blackListedTopologiesId.add(topologyJson.get("id").toString());
						}
					}

				}
				topologiesId.removeAll(blackListedTopologiesId);
				topologyMap.put(clusterName, topologiesId);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (JSONException e) {
				e.printStackTrace();
			}
		} else {
			for (String string : stormClusterMap.keySet()) {
				blackListedTopologies.clear();
				topologiesId.clear();
				summaryGet = new HttpGet(stormClusterMap.get(string) + STORM_PREFIX + SUMMARY);
				try {
					HttpResponse response = client.execute(summaryGet);
					HttpEntity respEntity = response.getEntity();
					if (respEntity != null) {
						content = EntityUtils.toString(respEntity);
					}
					JSONObject json = new JSONObject(content);
					JSONArray jsArr = json.getJSONArray("topologies");
					for (int i = 0; i < jsArr.length(); i++) {
						JSONObject topologyJson = jsArr.getJSONObject(i);
						Object obj = reader.getStormProps().get(string).get("blackListedTopologies");
						if (obj != null) {
							blackListedTopologies = Arrays.asList(String.valueOf(obj).split(","));
						}
						topologiesId.add(topologyJson.getString("id").toString());
						for (String topologies : blackListedTopologies) {
							if (topologyJson.getString("id").contains(topologies)) {
								blackListedTopologiesId.add(topologyJson.get("id").toString());
							}
						}

					}
					topologiesId.removeAll(blackListedTopologiesId);
					topologyMap.put(string, topologiesId);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}
		}
		return topologyMap;
	}

	public List<StormModel> getAllTopologyStats(String clusterName) {
		List<StormModel> topologyStats = new ArrayList<>();
		Map<String, List<String>> stormCluster = new HashMap<>();
		if (clusterName != null && !clusterName.isEmpty()) {
			stormCluster = getAllTopologyIds(clusterName);
		} else {
			stormCluster = getAllTopologyIds(null);
		}
		for (Map.Entry<String, List<String>> entry : stormCluster.entrySet()) {
			for (String topology : entry.getValue()) {
				topologyStats.add(getTopologyConfig(topology, entry.getKey()));
			}
		}
		return topologyStats;
	}

	public StormModel getTopologyStat(String topologyId, String clusterName) {
		return zooMonitor.getZookeeperData(getTopologyConfig(topologyId, clusterName));
	}

	private StormModel getTopologyConfig(String topologyId, String clusterName) {
		StormModel model = new StormModel();
		HttpClient client = HttpClientBuilder.create().build();
		String content = null;
		HttpGet summaryGet = new HttpGet(stormClusterMap.get(clusterName) + STORM_PREFIX + "/" + topologyId);
		try {
			HttpResponse response = client.execute(summaryGet);
			HttpEntity respEntity = response.getEntity();
			if (respEntity != null) {
				content = EntityUtils.toString(respEntity);
			}
			JSONObject json = new JSONObject(content);
			JSONObject configJson = json.getJSONObject("configuration");
			model.setConsumerId(configJson.has("consumer-id") ? configJson.get("consumer-id").toString() : null);
			model.setTopic(configJson.has("topic") ? configJson.get("topic").toString() : null);
			model.setZkRoot(configJson.has("zk-root") ? configJson.get("zk-root").toString() : "/kafkastorm");
			model.setTopologyId(topologyId);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return model;
	}

	public List<String> getActiveConsumers() {
		return null;
	}

}
