package com.github.kafka.monitor.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Component;


@Component
public class PropertyReader {

	private int noOfKafkaClusters;
	private int noOfStormClusters;
	private HashMap<String, Map<String, String>> kafkaPropsMap = new HashMap<String,Map<String,String>>();
	private HashMap<String, Map<String, String>> stormPropsMap = new HashMap<String,Map<String,String>>();
	private HashMap<String, Object> generalConfig = new HashMap<>();
	private HashMap<String, Object> slackConfigMap = new HashMap<>();
	private HashMap<String, Object> mailConfigMap = new HashMap<>();

	public void readFile() throws IOException {
		try {
			InputStream url = PropertyReader.class.getResourceAsStream("/props.json");
			String value = readFromInputStream(url);
			JSONObject json = new JSONObject(value);
			JSONArray kafkaArray = json.getJSONArray("kafka");
			noOfKafkaClusters = kafkaArray.length();
			for (int i = 0; i < noOfKafkaClusters; i++) {
				JSONObject kafkaConfig = kafkaArray.getJSONObject(i);
				Map<String,String> kafkaMap = new HashMap<>();
				kafkaMap.put("clusterName",kafkaConfig.getString("clusterName"));
				kafkaMap.put("bootstrapServers",kafkaConfig.getString("bootstrapServers"));
				kafkaMap.put("groupId",kafkaConfig.getString("groupId"));
				kafkaMap.put("keyDeserializer",kafkaConfig.getString("keyDeserializer"));
				kafkaMap.put("valueDeserializer",kafkaConfig.getString("valueDeserializer"));
				kafkaPropsMap.put(kafkaConfig.getString("clusterName"), kafkaMap);
			}
			
			JSONArray stormArray = json.getJSONArray("storm");
			noOfStormClusters = stormArray.length();
			for (int i = 0; i < noOfStormClusters; i++) {
				JSONObject stormConfig = stormArray.getJSONObject(i);
				Map<String,String> stormMap = new HashMap<>();
				stormMap.put("clusterName",stormConfig.getString("clusterName"));
				stormMap.put("stormUrl",stormConfig.getString("stormUrl"));
				stormMap.put("zookeeperServers",stormConfig.getString("zookeeperServers"));
				stormMap.put("blackListedTopologies",stormConfig.getString("blackListedTopologies"));
				stormPropsMap.put(stormConfig.getString("clusterName"), stormMap);
			}
			
			JSONObject general = json.getJSONObject("general");
			generalConfig.put("refreshInterval", general.get("refreshInterval"));
			generalConfig.put("slackToggle", general.get("slackToggle"));
			generalConfig.put("emailToggle", general.get("emailToggle"));
			
			JSONObject slackConfig = json.getJSONObject("slack");
			if(slackConfig!=null) {
				slackConfigMap.put("url", slackConfig.get("webHookUrl"));
			}
			
			JSONObject mailConfig = json.getJSONObject("email");
			if(mailConfig!=null) {
				mailConfigMap.put("host", mailConfig.get("mailHost"));
				mailConfigMap.put("port", mailConfig.get("mailPort"));
				mailConfigMap.put("from", mailConfig.get("from"));
				mailConfigMap.put("to", mailConfig.get("to"));
				mailConfigMap.put("subject", mailConfig.get("subject"));
				
			}
			
		} catch (JSONException e) {
			e.printStackTrace();
		}

	}
	
	public HashMap<String, Map<String, String>> getKafkaProps() {
		return kafkaPropsMap;
	}
	
	public HashMap<String, Map<String, String>> getStormProps() {
		return stormPropsMap;
	}
	
	public HashMap<String, Object> getGeneralConfigProps() {
		return generalConfig;
	}
	
	public HashMap<String, Object> getEmailConfigProps() {
		return mailConfigMap;
	}
	
	public HashMap<String, Object> getSlackConfigProps() {
		return slackConfigMap;
	}

	private String readFromInputStream(InputStream inputStream) throws IOException {
		StringBuilder resultStringBuilder = new StringBuilder();
		try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
			String line;
			while ((line = br.readLine()) != null) {
				resultStringBuilder.append(line).append("\n");
			}
		}
		return resultStringBuilder.toString();
	}
}
