package com.yuyuko.paxoskv.core;

import java.util.Arrays;

public class ChosenValue {
    private final long instanceId;

    private final long promiseId;

    private final long promiseNodeId;

    private final long acceptedId;

    private final long acceptedNodeId;

    private final byte[] acceptedValue;

    public ChosenValue(long instanceId, long promiseId, long promiseNodeId, long acceptedId,
                       long acceptedNodeId, byte[] acceptedValue) {
        this.instanceId = instanceId;
        this.promiseId = promiseId;
        this.promiseNodeId = promiseNodeId;
        this.acceptedId = acceptedId;
        this.acceptedNodeId = acceptedNodeId;
        this.acceptedValue = acceptedValue;
    }

    //for test
    public ChosenValue(long instanceId) {
        this(instanceId, 0, 0, 0, 0, "".getBytes());
    }

    public long getInstanceId() {
        return instanceId;
    }

    public long getPromiseId() {
        return promiseId;
    }

    public long getPromiseNodeId() {
        return promiseNodeId;
    }

    public long getAcceptedId() {
        return acceptedId;
    }

    public long getAcceptedNodeId() {
        return acceptedNodeId;
    }

    public byte[] getAcceptedValue() {
        return acceptedValue;
    }

    @Override
    public String toString() {
        return "ChosenValue{" +
                "instanceId=" + instanceId +
                ", promiseId=" + promiseId +
                ", promiseNodeId=" + promiseNodeId +
                ", acceptedId=" + acceptedId +
                ", acceptedNodeId=" + acceptedNodeId +
                ", acceptedValue.length=" + (acceptedValue == null ? null : acceptedValue.length) +
                '}';
    }
}
