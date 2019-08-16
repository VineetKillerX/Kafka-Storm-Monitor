package com.github.kafka.monitor.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;
@Service
public class MonitorRules {
	
	private static final Long MAX_LAG = 100000l;
	

	// Checking if over a window, lag is zero , then the state is Okay
	public boolean isLagAlwaysNotZero(LinkedHashMap<Long, Map<Integer, Long>> lagMap) {
		//System.out.println("Lag Map in isLagAlwaysNotZero "+lagMap.toString());
		int partitions = 0;
		for (Map.Entry<Long, Map<Integer, Long>> entry : lagMap.entrySet()) {
			partitions = entry.getValue().size();
			for (int i = 0; i < partitions; i++) {
				if (entry.getValue().get(i) == 0) {
					return false;
				}

			}
		}
		return true;
	}

	// Checking if Over a period of time, offset is rewinding
	public boolean checkIfOffsetsRewind(LinkedHashMap<Long, Map<Integer, Long>> consumerMaps) {
		//System.out.println("Consumer Map in checkIfOffsetsRewind "+consumerMaps.toString());
		if(consumerMaps.size()>1) {
			List<Map<Integer, Long>> list = new ArrayList<>(consumerMaps.values());
			int partitions = 0;
			for (int i = 1; i < list.size(); i++) {
				partitions = list.get(i).size();
				for (int j = 0; j < partitions; j++) {
					if (list.get(i).get(j) < list.get(i - 1).get(j)) {
						return true;
					}
				}
			}
			return false;
		}else {
			return false;
		}
		
	}
	
	
	public boolean checkLagThreshold(LinkedHashMap<Long, Map<Integer, Long>> lagMap) {
		Collection<Map<Integer, Long>> lagValues = lagMap.values();
		Map<Integer, Long> lastLagMap = (Map<Integer, Long>) lagValues.toArray()[lagValues.size()-1];
		for (Map.Entry<Integer, Long> entrySet : lastLagMap.entrySet()) {
			if(entrySet.getValue()>MAX_LAG) {
				return true;
			}
		}
		return false;
	}
	
	
	public boolean checkIfOffsetsStopped(LinkedHashMap<Long, Map<Integer, Long>> consumerMaps) {
		//System.out.println("Consumer Map in checkIfOffsetsStopped "+consumerMaps.toString());
		if(consumerMaps.size()>1) {
			List<Map<Integer,Long>> listOfOffsets = new ArrayList<>(consumerMaps.values());
			int partitions = listOfOffsets.get(0).size();
			for (int i = 0; i < partitions; i++) {
				if(listOfOffsets.get(0).get(i) == listOfOffsets.get(listOfOffsets.size()-1).get(i)) {
					return true;
				}
			}
			
			return false;
		}else {
			return false;
		}
		
	}

	// Checking if Over a period of time, offset is rewinding
	public boolean checkIfLagIsIncreasing(LinkedHashMap<Long, Map<Integer, Long>> consumerMaps,
			LinkedHashMap<Long, Map<Integer, Long>> lagMap) {
		System.out.println("Consumer Map in checkIfLagIsIncreasing "+consumerMaps.toString()+" And Lag Map "+lagMap.toString());
		List<Map<Integer, Long>> list = new ArrayList<>(consumerMaps.values());
		List<Map<Integer, Long>> lagList = new ArrayList<>(lagMap.values());
		if(lagMap.size()>1 && consumerMaps.size()>1) {
		int partitions = 0;
		for (int i = 1; i < list.size(); i++) {
			partitions = list.get(i).size();
			for (int j = 0; j < partitions; j++) {
				if (list.get(i).get(j) > list.get(i - 1).get(j) && lagList.get(i).get(j) >= lagList.get(i - 1).get(j)) {
					return true;
				}
			}
		}
		}else {
			return false;
		}
		return false;
	}

	public boolean checkIfLagNotDecreasing(LinkedHashMap<Long, Map<Integer, Long>> lagMap) {
		//System.out.println("Lag Map in checkIfLagNotDecreasing "+lagMap.toString());
		int partitions = 0;
		if(lagMap.size()>1) {
			for (Map.Entry<Long, Map<Integer, Long>> entry : lagMap.entrySet()) {
				partitions = entry.getValue().size();
				for (int i = 1; i < partitions; i++) {
					if (entry.getValue().get(i) < entry.getValue().get(i - 1)) {
						return true;
					}

				}
			}
		}else {
			return false;
		}
		
		return false;
	}

	public boolean checkIfRecentLagWasZero(LinkedHashMap<Long, Map<Integer, Long>> consumerMaps,
			Map<Integer, Long> brokerOffset) {
		//System.out.println("Consumer Map in checkIfRecentLagWasZero "+consumerMaps.toString()+" With Broker Offsets "+brokerOffset.toString());
		int partitions = brokerOffset.size();
		List<Map<Integer, Long>> list = new ArrayList<>(consumerMaps.values());
		Map<Integer, Long> lastOffsetMap = list.get(list.size() - 1);
		for (int i = 0; i < partitions; i++) {
			if (brokerOffset.get(i) <= lastOffsetMap.get(i)) {
				return true;
			}
		}
		return false;
	}

}
