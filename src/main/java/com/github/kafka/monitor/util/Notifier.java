package com.github.kafka.monitor.util;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

@Component
public class Notifier {
	
	Logger LOGGER = LoggerFactory.getLogger(Notifier.class);

	@Autowired
	private JavaMailSender javaMailSender;
	
	@Autowired
	PropertyReader props;

	public void sendEmail(String status, String consumerId, String topic) {
		SimpleMailMessage msg = new SimpleMailMessage();
		msg.setTo(props.getEmailConfigProps().get("to").toString().split(","));
		msg.setSubject(props.getEmailConfigProps().get("subject").toString());
		msg.setFrom(props.getEmailConfigProps().get("from").toString());
		msg.setText("Consumer : " + consumerId + "reading from Topic : " + topic + " is in " + status + " state");
		javaMailSender.send(msg);
	}

	public void sendSlackNotifications(String status, String consumerId, String topic, Map map, String topologyId) {
		Map<Integer, Long> lastValue = null;
		if (map != null && !map.isEmpty()) {
			lastValue = (Map<Integer, Long>) map.values().toArray()[map.size() - 1];
		}
		String text = "Consumer : " + consumerId + " with topology Id : " + topologyId + " reading from Topic : "
				+ topic + " is in " + status + " state with Current Lag is "+new JSONObject(lastValue) ;
		JSONObject json = new JSONObject();
		try {
			json.put("text", text);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		JSONArray jsonArr = templateSlackNotification(status, consumerId, topic, map);
		LOGGER.debug("Slack Notification Message : {}",jsonArr.toString());
		Map<String,String> headersMap = new HashMap<>();
		headersMap.put("content-type","application/json");
		//HttpUtil.sendPostRequest(jsonArr.toString(),props.getSlackConfigProps().get("url").toString(),headersMap,false);
		//NOTE : Uncomment the below line and comment the above line in case when slack is not able to send notifications.
		HttpUtil.sendPostRequest(json.toString(),props.getSlackConfigProps().get("url").toString(),headersMap,false);
	}

	private JSONArray templateSlackNotification(String status, String consumerId, String topic, Map map) {
		JSONArray jsArr = new JSONArray();
		Map<Integer, Long> lastValue = null;
		if (map != null && !map.isEmpty()) {
			lastValue = (Map<Integer, Long>) map.values().toArray()[map.size() - 1];
		}
		int size = lastValue.size();
		int numberOfSections = (size / 10) + 1;
		int currentPartition = 0;
		try {
			JSONObject msgJson = new JSONObject();
			msgJson.put("type", "section");
			JSONObject text = new JSONObject();
			text.put("type", "mrkdwn");
			text.put("text", "Consumer : *" + consumerId + "* , consuming messages from Topic : *" + topic + "* is in *"
					+ status + "* state");
			msgJson.put("text", text);
			jsArr.put(msgJson);
			JSONObject divider = new JSONObject();
			divider.put("type", "divider");
			jsArr.put(divider);
			// Adding Header Section

			JSONObject headerJson = new JSONObject();
			headerJson.put("type", "section");
			JSONArray fieldsArr = new JSONArray();
			JSONObject fieldText = new JSONObject();
			fieldText.put("type", "mrkdwn");
			fieldText.put("text", "*Partition*");
			fieldsArr.put(fieldText);
			JSONObject fieldTextValue = new JSONObject();
			fieldTextValue.put("type", "mrkdwn");
			fieldTextValue.put("text", "*Lag*");
			fieldsArr.put(fieldTextValue);
			headerJson.put("fields", fieldsArr);
			jsArr.put(headerJson);

			// Adding Partition Map
			for (int i = 0; i < numberOfSections; i++) {
				JSONObject valuesJson = new JSONObject();
				valuesJson.put("type", "section");
				JSONArray valuesArr = new JSONArray();
				for (int j = 0; j < 5; j++) {
					JSONObject keyJson = new JSONObject();
					keyJson.put("type", "plain_text");
					keyJson.put("text", String.valueOf(currentPartition));

					JSONObject valueJson = new JSONObject();
					valueJson.put("type", "plain_text");
					valueJson.put("text", String.valueOf(lastValue.get(currentPartition)));

					valuesArr.put(keyJson);
					valuesArr.put(valueJson);
					currentPartition++;
					
				}
				valuesJson.put("fields", valuesArr);
				jsArr.put(valuesJson);

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsArr;
	}
}
