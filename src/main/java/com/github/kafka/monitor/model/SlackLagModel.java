package com.github.kafka.monitor.model;

import java.util.Map;

public class SlackLagModel {

    private String topologyId;
    private Map lagMap;

    public SlackLagModel(String topologyId, Map lagMap) {
        this.topologyId = topologyId;
        this.lagMap = lagMap;
    }

    public String getTopologyId() {
        return topologyId;
    }

    public Map getLagMap() {
        return lagMap;
    }

}
