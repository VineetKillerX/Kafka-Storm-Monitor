package com.github.kafka.monitor.config;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import com.github.kafka.monitor.model.NewRelicModel;
import com.github.kafka.monitor.model.StormModel;
import com.github.kafka.monitor.model.TopicModel;
import com.github.kafka.monitor.service.KafkaConsumerMonitor;
import com.github.kafka.monitor.service.StormMonitor;
import com.github.kafka.monitor.service.ZookeeperMonitor;
import com.github.kafka.monitor.util.PropertyReader;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.gson.Gson;

@Configuration
public class ApplicationCache {

	private Cache<String, Object> offsetsCache;

	private Cache<String, Object> topicCache;

	private Cache<String, Object> topologyCache;

	private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationCache.class);

	DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
	SimpleDateFormat sdFormat = new SimpleDateFormat("yyyyMMddHHmmss");

	List<NewRelicModel> listOfNewRelicModels = new ArrayList<NewRelicModel>();

	private Map<String, String> headersMapForNewRelic = new HashMap<>();

	@Autowired
	ZookeeperMonitor zooMonitor;

	@Autowired
	StormMonitor stormMonitor;

	@Autowired
	KafkaConsumerMonitor kafkaMonitor;

	@Autowired
	PropertyReader property;
	
	Gson gson = new Gson();

	@PostConstruct
	private void initCache() {
		RemovalListener<String, Object> listener = new RemovalListener<String, Object>() {
			@Override
			public void onRemoval(RemovalNotification<String, Object> notification) {
				if (notification.getCause() == RemovalCause.EXPIRED) {
					topicCache.put("topics", kafkaMonitor.getAllTopicsDetails(null));
				}
			}
		};
		RemovalListener<String, Object> topologyListener = new RemovalListener<String, Object>() {
			@Override
			public void onRemoval(RemovalNotification<String, Object> notification) {
				if (notification.getCause() == RemovalCause.EXPIRED) {
					topologyCache.put("topologies", stormMonitor.getAllTopologyStats(null));
				}
			}
		};
		offsetsCache = CacheBuilder.newBuilder().build();
		topicCache = CacheBuilder.newBuilder().expireAfterWrite(Duration.ofMinutes(5)).removalListener(listener)
				.build();
		topologyCache = CacheBuilder.newBuilder().expireAfterWrite(Duration.ofMinutes(30))
				.removalListener(topologyListener).build();
		headersMapForNewRelic.put("content-type", "application/gzip");
		headersMapForNewRelic.put("cache-control", "no-cache");
	}

	public Map<String, Object> getCacheDetails() {
		Map<String, Object> map = new HashMap<>();
		map.put("offsetsCache", offsetsCache.asMap());
		map.put("topicCache", topicCache.asMap());
		map.put("topologyCache", topologyCache.asMap());
		return map;
	}

	public void addToTopicOffsetCache(String topic, Map<Integer, Long> offsets) {
		topicCache.put(topic, offsets);
	}

	public List<TopicModel> getAllTopicsInfo() {
		return (List<TopicModel>) topicCache.getIfPresent("topics");
	}

	@PostConstruct
	public void loadCache() {
		try {
			property.readFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		topicCache.put("topics", kafkaMonitor.getAllTopicsDetails(null));
	}

	public Map<Integer, Long> getFromTopicOffsetCache(String topic, String clusterName) throws ExecutionException {
		Object obj = topicCache.getIfPresent(topic);
		Map<Integer, Long> partitonsMap = null;
		if (obj == null) {
			List<TopicModel> topicInfo = null;
			if (topicCache.getIfPresent("topics") == null) {
				synchronized (this) {
					if (topicCache.getIfPresent("topics") == null) {
						topicInfo = kafkaMonitor.getAllTopicsDetails(null);
						topicCache.put("topics", topicInfo);
					}
				}
			} else {
				topicInfo = (List<TopicModel>) topicCache.getIfPresent("topics");
			}
			for (TopicModel topicModel : topicInfo) {
				if (topicModel.getName() != null && topicModel.getName().equalsIgnoreCase(topic)) {
					partitonsMap = topicModel.getPartitionsMap();
					addToTopicOffsetCache(topic, partitonsMap);
					obj = topicCache.getIfPresent(topic);
				}
			}

		} else {
			partitonsMap = (Map<Integer, Long>) obj;
		}
		return partitonsMap;
	}

	public void addToConsumerOffsetCache(String consumerId, Map<Integer, Long> consumerOffsets)
			throws ExecutionException {
		Object obj = offsetsCache.getIfPresent(consumerId);
		Long format = generateKey();
		LinkedHashMap<Long, Map<Integer, Long>> offsetMap = null;
		if (obj != null) {
			offsetMap = (LinkedHashMap<Long, Map<Integer, Long>>) obj;
			int currentSize = offsetMap.size();
			if (currentSize > 10) {
				offsetMap.remove(offsetMap.entrySet().iterator().next().getKey());
			}
			offsetMap.put(format, consumerOffsets);
		} else {
			offsetMap = new LinkedHashMap<>();
			offsetMap.put(format, consumerOffsets);
		}
		offsetsCache.put(consumerId, offsetMap);
	}

	public void addToOffsetLagCache(String consumerId, Map<Integer, Long> consumerOffsets) throws ExecutionException {
		Object obj = offsetsCache.getIfPresent("lag_" + consumerId);
		Long format = generateKey();
		LinkedHashMap<Long, Map<Integer, Long>> offsetMap = null;
		if (obj != null) {
			offsetMap = (LinkedHashMap<Long, Map<Integer, Long>>) obj;
			int currentSize = offsetMap.size();
			if (currentSize > 10) {
				offsetMap.remove(offsetMap.entrySet().iterator().next().getKey());
			}
			offsetMap.put(format, consumerOffsets);
		} else {
			offsetMap = new LinkedHashMap<>();
			offsetMap.put(format, consumerOffsets);
		}
		offsetsCache.put("lag_" + consumerId, offsetMap);
	}

	public LinkedHashMap<Long, Map<Integer, Long>> getConsumerOffsets(String consumerId, String zkRoot)
			throws ExecutionException, ParseException {
		Object obj = offsetsCache.getIfPresent(consumerId);
		StormModel stormModel = new StormModel();
		stormModel.setConsumerId(consumerId);
		stormModel.setZkRoot(zkRoot);
		if (obj == null) {
			stormModel = zooMonitor.getZookeeperData(stormModel);
			Map<Integer, Long> offsets = stormModel.getOffsetMap();
			addToConsumerOffsetCache(consumerId, offsets);
			obj = offsetsCache.getIfPresent(consumerId);
		} else {
			LinkedHashMap<Long, Map<Integer, Long>> offsetMap = (LinkedHashMap<Long, Map<Integer, Long>>) obj;
			long lastKey = Long.valueOf(offsetMap.keySet().toArray()[offsetMap.size() - 1].toString());
			Long currentKey = generateKey();
			if (Math.abs(currentKey - lastKey) > 60) {
				stormModel = zooMonitor.getZookeeperData(stormModel);
				Map<Integer, Long> offsets = stormModel.getOffsetMap();
				addToConsumerOffsetCache(consumerId, offsets);
				obj = offsetsCache.getIfPresent(consumerId);
			}
		}
		LinkedHashMap<Long, Map<Integer, Long>> offsetMap = (LinkedHashMap<Long, Map<Integer, Long>>) obj;
		return offsetMap;
	}

	// Rewrite
	public LinkedHashMap<Long, Map<Integer, Long>> getConsumerLag(String consumerId, String topic, String zkRoot)
			throws ExecutionException, ParseException {
		LinkedHashMap<Long, Map<Integer, Long>> offsetMap = null;
		Object obj = offsetsCache.getIfPresent("lag_" + consumerId);
		if (obj == null) {
			LinkedHashMap<Long, Map<Integer, Long>> consumerMap = getConsumerOffsets(consumerId, zkRoot);
			Long lastKey = (Long) consumerMap.keySet().toArray()[consumerMap.size() - 1];
			Map<Integer, Long> consumerOffsets = consumerMap.get(lastKey);
			Map<Integer, Long> topicOffsets = getFromTopicOffsetCache(topic, null);
			Map<Integer, Long> lagMap = new HashMap<>();
			int partitions = topicOffsets.size();
			for (int i = 0; i < partitions; i++) {
				long lag = topicOffsets.get(i) - consumerOffsets.get(i);
				lagMap.put(i, lag);
			}
			offsetMap = new LinkedHashMap<>();
			offsetMap.put(lastKey, lagMap);
			addToOffsetLagCache(consumerId, lagMap);
		} else {
			offsetMap = (LinkedHashMap<Long, Map<Integer, Long>>) obj;
			long lastKey = Long.valueOf(offsetMap.keySet().toArray()[offsetMap.size() - 1].toString());
			Long currentKey = generateKey();
			if (Math.abs(currentKey - lastKey) > 60) {
				LinkedHashMap<Long, Map<Integer, Long>> consumerMap = getConsumerOffsets(consumerId, zkRoot);
				Long latestKey = (Long) consumerMap.keySet().toArray()[consumerMap.size() - 1];
				Map<Integer, Long> consumerOffsets = consumerMap.get(latestKey);
				Map<Integer, Long> topicOffsets = getFromTopicOffsetCache(topic, null);
				Map<Integer, Long> lagMap = new HashMap<>();
				int partitions = topicOffsets.size();
				for (int i = 0; i < partitions; i++) {
					long lag = topicOffsets.get(i) - consumerOffsets.get(i);
					lagMap.put(i, lag);
				}
				addToOffsetLagCache(consumerId, lagMap);
				offsetMap.put(currentKey, lagMap);
			}
		}
		return offsetMap;
	}

	public List<StormModel> getTopologyConsumerInfo() throws ExecutionException {
		List<StormModel> topologyInfo = null;
		Object obj = topologyCache.getIfPresent("topologies");
		if (obj == null) {
			topologyInfo = stormMonitor.getAllTopologyStats(null);
			addTopologyConsumerInfoToCache(topologyInfo);
		} else {
			topologyInfo = (List<StormModel>) obj;
		}
		return topologyInfo;
	}

	public void addTopologyConsumerInfoToCache(List<StormModel> info) throws ExecutionException {
		topologyCache.put("topologies", info);
	}

	private Long generateKey() {
		LocalDateTime ldt = LocalDateTime.now();
		Long currentKey = Long.valueOf(ldt.format(format));
		return currentKey;
	}

	public Map<String, List<String>> getListOfTopics() {
		Map<String, List<String>> topicsMap = null;
		Object obj = topicCache.getIfPresent("listOfTopics");
		if (obj == null) {
			topicsMap = kafkaMonitor.getTopics();
			topicCache.put("listOfTopics", topicsMap);
		} else {
			topicsMap = (Map<String, List<String>>) obj;
		}
		return topicsMap;

	}
}
