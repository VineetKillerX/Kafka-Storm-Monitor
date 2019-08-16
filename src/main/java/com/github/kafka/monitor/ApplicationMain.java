package com.github.kafka.monitor;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import com.github.kafka.monitor.config.ApplicationCache;
import com.github.kafka.monitor.model.StormModel;
import com.github.kafka.monitor.util.MonitorRules;
import com.github.kafka.monitor.util.Notifier;
import com.github.kafka.monitor.util.PropertyReader;

@Component
public class ApplicationMain {
	
	Logger LOGGER = LoggerFactory.getLogger(ApplicationMain.class);

	@Autowired
	ApplicationCache cache;

	@Autowired
	MonitorRules rulesEngine;

	@Autowired
	Notifier notifier;
	
	@Autowired
	PropertyReader reader;
	
	public boolean isMailEnabled = false;
	public boolean isSlackEnabled = false;

	private String getStatus(StormModel stormModel) {
		try {
			List<String> topicsList = new ArrayList<>();
			if (stormModel.getConsumerId() != null && stormModel.getTopic() != null) {
				cache.getListOfTopics().values().forEach(e -> topicsList.addAll(e));
				if (topicsList.contains(stormModel.getTopic())) {
					LOGGER.info("Monitoring Consumer : {} consuming messages from Topic : {} " , stormModel.getConsumerId(), stormModel.getTopic());
					Map<Integer, Long> topicMap = cache.getFromTopicOffsetCache(stormModel.getTopic(), null);
					LinkedHashMap<Long, Map<Integer, Long>> consumerMap = cache
							.getConsumerOffsets(stormModel.getConsumerId(), stormModel.getZkRoot());
					LinkedHashMap<Long, Map<Integer, Long>> lagMap = cache.getConsumerLag(stormModel.getConsumerId(),
							stormModel.getTopic(), stormModel.getZkRoot());
					boolean isOffsetZero = topicMap.values().stream().anyMatch(e -> e == 0);
					if (isOffsetZero) {
						return "OK";
					}
					if(rulesEngine.checkLagThreshold(lagMap)) {
						return "LAG";
					}
					Boolean offsetStopped = rulesEngine.checkIfOffsetsStopped(consumerMap);
					Boolean recentLagZero = rulesEngine.checkIfRecentLagWasZero(consumerMap, topicMap);
					if (offsetStopped && !recentLagZero) {
						return "STOPPED";
					}
					/*if(rulesEngine.checkIfLagIsIncreasing(consumerMap, lagMap)) {
						System.out.println("Lag is Increasing");
						return "WARNING";
					}*/
					Boolean isLagAlwaysNonZero = rulesEngine.isLagAlwaysNotZero(lagMap);
					if (isLagAlwaysNonZero) {
						/*if (rulesEngine.checkIfOffsetsRewind(consumerMap)) {
							return "REWIND";
						}*/
						if (rulesEngine.checkIfOffsetsStopped(consumerMap)) {
							return "STALLED";
						}
					/*	if (rulesEngine.checkIfLagNotDecreasing(lagMap)) {
							return "WARNING";
						}*/
					}

				}
			}
		} catch (Exception e) {
			LOGGER.error("Exception occured in getStatus for model : {} with Error : {}",stormModel.toString(),e);
			return "ERROR";
		}
		return "OK";
	}

	@EventListener(ApplicationReadyEvent.class)
	public void monitor() throws ExecutionException, InterruptedException, ParseException {
		isMailEnabled = Boolean.parseBoolean(reader.getGeneralConfigProps().get("emailToggle").toString());
		isSlackEnabled = Boolean.parseBoolean(reader.getGeneralConfigProps().get("slackToggle").toString());
		LOGGER.info("Mail Enabled : {}", isMailEnabled);
		LOGGER.info("Slack Enabled : {}", isSlackEnabled);
		while (true) {
			List<StormModel> stormConsumers = cache.getTopologyConsumerInfo();
			for (StormModel stormModel : stormConsumers) {
				String status = getStatus(stormModel);
				LOGGER.info("Status is {} for stormModel : {}",status,stormModel.toString());
				if (!status.equals("OK") && !status.equals("ERROR")) {
					if(isMailEnabled)
						notifier.sendEmail(status, stormModel.getConsumerId(), stormModel.getTopic());
					if(isSlackEnabled)
						notifier.sendSlackNotifications(status, stormModel.getConsumerId(), stormModel.getTopic(), cache.getConsumerLag(stormModel.getConsumerId(), stormModel.getTopic(), stormModel.getZkRoot()), stormModel.getTopologyId());
				}

			}
			Long refreshRate = Long.parseLong(reader.getGeneralConfigProps().get("refreshInterval").toString());
			LOGGER.info("Refresh Rate Configured is : {}",refreshRate);
			Thread.sleep(1000 * refreshRate);

		}
	}
}
