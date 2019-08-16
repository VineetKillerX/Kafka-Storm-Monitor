package com.github.kafka.monitor.service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.utils.Bytes;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import com.github.kafka.monitor.model.StormModel;


@Service
public class ZookeeperMonitor {

	private ZooKeeper zoo;
	CountDownLatch connectionLatch = new CountDownLatch(1);

	private ZooKeeper connect(String host) throws IOException, InterruptedException {
		if(zoo == null) {
			zoo = new ZooKeeper(host, 1000*60*2, new Watcher() {
				public void process(WatchedEvent we) {
					if (we.getState() == KeeperState.SyncConnected) {
						connectionLatch.countDown();
					}
				}
			});
			connectionLatch.await();
		}
		return zoo;
	}
	
	public void connectToZookeeper(String hosts,int retries) {
		try {
			connect(hosts);
		} catch (Exception e) {
			retries++;
			if(retries<3) {
				connectToZookeeper(hosts,retries);
			}
			throw new RuntimeException("Error While Connecting to Zookeeper",e);
		}
	}
	
	public StormModel getZookeeperData(StormModel model) {
		StringBuilder broker = new StringBuilder();
		try {
			connectToZookeeper("zookeeper.abc",0);
			if(model.getZkRoot()==null) {
				model.setZkRoot("/kafkastorm");
			}
			String zkPath = model.getZkRoot()+"/"+model.getConsumerId();
			List<String> nodes = zoo.getChildren(zkPath, false);
			Map<Integer,Long> offsetMap = new HashMap<>();
			for (String node : nodes) {
				byte[] data = zoo.getData(zkPath+"/"+node, false, null);
				JSONObject json = new JSONObject(Bytes.wrap(data).toString());
				offsetMap.put(json.getInt("partition"), json.getLong("offset"));
				JSONObject brokerObj = json.getJSONObject("broker");
				broker.append(brokerObj.getString("host")+":"+brokerObj.getString("port"));
				broker.append(",");
			}
			broker.deleteCharAt(broker.length()-1);
			model.setBrokers(broker.toString());
			model.setOffsetMap(offsetMap);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return model;
	}
	
	

}
