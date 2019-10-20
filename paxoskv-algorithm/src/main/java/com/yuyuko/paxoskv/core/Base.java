package com.yuyuko.paxoskv.core;

import com.yuyuko.paxoskv.core.node.Config;

import java.util.ArrayList;
import java.util.List;

public abstract class Base {
    protected List<PaxosMessage> messages = new ArrayList<>();

    protected final PaxosInstance instance;

    protected long instanceId;

    protected final Config config;

    public Base(PaxosInstance instance, Config config) {
        this.config = config;
        this.instance = instance;
    }

    public long getInstanceId() {
        return instanceId;
    }

    public void newInstance() {
        ++instanceId;
        initForNewPaxosInstance();
    }

    public void sendMessage(PaxosMessage.Builder builder) {
        messages.add(builder.build());
    }

    public void broadcastMessage(PaxosMessage.Builder builder) {
        config.getNodeInfoList().forEach(nodeInfo -> {
            PaxosMessage message = builder.toNodeId(nodeInfo.getNodeId()).build();
            messages.add(message);
        });
    }

    public void setInstanceId(long instanceId) {
        this.instanceId = instanceId;
    }

    List<PaxosMessage> readMessages() {
        try {
            return messages;
        } finally {
            messages = new ArrayList<>();
        }
    }

    public List<PaxosMessage> getMessages() {
        return messages;
    }

    protected abstract void initForNewPaxosInstance();

    public void clearMessages() {
        messages = new ArrayList<>();
    }
}