package com.yuyuko.paxoskv.core.node;

import com.yuyuko.paxoskv.core.storage.Storage;

import java.util.List;

public class Config {
    private long nodeId;

    private int nodeCount;

    private List<NodeInfo> nodeInfoList;

    private Storage storage;

    private long prepareTimeout = 2000;

    private long acceptTimeout = 2000;

    private long askForLearnTimeout = 1000;

    private long restartPrepareTimeout = 30;

    private long restartAcceptTimeout = 30;

    public int quorum() {
        return nodeCount / 2 + 1;
    }


    public long getNodeId() {
        return nodeId;
    }

    public int getNodeCount() {
        return nodeCount;
    }

    public List<NodeInfo> getNodeInfoList() {
        return nodeInfoList;
    }

    public Storage getStorage() {
        return storage;
    }

    public long getPrepareTimeout() {
        return prepareTimeout;
    }

    public long getAcceptTimeout() {
        return acceptTimeout;
    }

    public long getAskForLearnTimeout() {
        return askForLearnTimeout;
    }

    public long getRestartPrepareTimeout() {
        return restartPrepareTimeout;
    }

    public long getRestartAcceptTimeout() {
        return restartAcceptTimeout;
    }

    public void setNodeId(long nodeId) {
        this.nodeId = nodeId;
    }

    public void setNodeCount(int nodeCount) {
        this.nodeCount = nodeCount;
    }

    public void setNodeInfoList(List<NodeInfo> nodeInfoList) {
        this.nodeInfoList = nodeInfoList;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    public void setAskForLearnTimeout(long askForLearnTimeout) {
        this.askForLearnTimeout = askForLearnTimeout;
    }

    public void setPrepareTimeout(long prepareTimeout) {
        this.prepareTimeout = prepareTimeout;
    }

    public void setAcceptTimeout(long acceptTimeout) {
        this.acceptTimeout = acceptTimeout;
    }

    public void setRestartPrepareTimeout(long restartPrepareTimeout) {
        this.restartPrepareTimeout = restartPrepareTimeout;
    }

    public void setRestartAcceptTimeout(long restartAcceptTimeout) {
        this.restartAcceptTimeout = restartAcceptTimeout;
    }
}
