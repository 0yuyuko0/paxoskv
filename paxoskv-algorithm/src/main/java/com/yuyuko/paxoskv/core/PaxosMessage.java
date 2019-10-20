package com.yuyuko.paxoskv.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class PaxosMessage {
    private PaxosMessageType type;

    private long instanceId;

    private long nodeId;

    private long toNodeId;

    private long proposalId;

    private long proposalNodeId;

    private byte[] value;

    private long preAcceptId;

    private long preAcceptNodeId;

    private long rejectByPromiseId;

    private long nowInstanceId;

    private long minChosenInstanceId;

    private List<ChosenValue> learnedValues;

    public enum PaxosMessageType {
        Prepare,
        PrepareReply,
        Accept,
        AcceptReply,
        AskForLearn,
        SendLearnValue,
        ProposerSendSuccess,
    }

    public static Builder builder() {
        return new Builder(new PaxosMessage());
    }

    public static Builder builder(PaxosMessage m){
        return new Builder(m.copy());}

    public static class Builder {
        private PaxosMessage message;

        public Builder(PaxosMessage message) {
            this.message = message;
        }

        public Builder type(PaxosMessageType type) {
            message.type = type;
            return this;
        }

        public Builder instanceId(long instanceId) {
            message.instanceId = instanceId;
            return this;
        }

        public Builder nodeId(long nodeId) {
            message.nodeId = nodeId;
            return this;
        }

        public Builder toNodeId(long toNodeId) {
            message.toNodeId = toNodeId;
            return this;
        }

        public Builder proposalId(long proposalId) {
            message.proposalId = proposalId;
            return this;
        }

        public Builder value(byte[] value) {
            message.value = value;
            return this;
        }

        public Builder preAcceptId(long preAcceptId) {
            message.preAcceptId = preAcceptId;
            return this;
        }

        public Builder preAcceptNodeId(long preAcceptNodeId) {
            message.preAcceptNodeId = preAcceptNodeId;
            return this;
        }

        public Builder rejectByPromiseId(long rejectByPromiseId) {
            message.rejectByPromiseId = rejectByPromiseId;
            return this;
        }

        public Builder learnedValues(List<ChosenValue> learnedValues) {
            message.learnedValues = learnedValues;
            return this;
        }



        public PaxosMessage build() {
            return message.copy();
        }
    }

    public PaxosMessageType getType() {
        return type;
    }

    public long getInstanceId() {
        return instanceId;
    }

    public long getNodeId() {
        return nodeId;
    }

    public long getProposalId() {
        return proposalId;
    }

    public byte[] getValue() {
        return value;
    }

    public long getPreAcceptId() {
        return preAcceptId;
    }

    public long getPreAcceptNodeId() {
        return preAcceptNodeId;
    }

    public long getRejectByPromiseId() {
        return rejectByPromiseId;
    }

    public List<ChosenValue> getLearnedValues() {
        return learnedValues;
    }

    public long getToNodeId() {
        return toNodeId;
    }

    public PaxosMessage copy() {
        PaxosMessage message = new PaxosMessage();
        message.type = type;
        message.instanceId = instanceId;
        message.nodeId = nodeId;
        message.toNodeId = toNodeId;
        message.proposalId = proposalId;
        message.proposalNodeId = proposalNodeId;
        //防止内存泄漏
        message.learnedValues = learnedValues == null ? null : new ArrayList<>(learnedValues);
        message.value = value == null ? null : value.clone();
        message.preAcceptId = preAcceptId;
        message.preAcceptNodeId = preAcceptNodeId;
        message.rejectByPromiseId = rejectByPromiseId;
        message.nowInstanceId = nowInstanceId;
        message.minChosenInstanceId = minChosenInstanceId;
        return message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PaxosMessage that = (PaxosMessage) o;
        return instanceId == that.instanceId &&
                nodeId == that.nodeId &&
                toNodeId == that.toNodeId &&
                proposalId == that.proposalId &&
                proposalNodeId == that.proposalNodeId &&
                preAcceptId == that.preAcceptId &&
                preAcceptNodeId == that.preAcceptNodeId &&
                rejectByPromiseId == that.rejectByPromiseId &&
                nowInstanceId == that.nowInstanceId &&
                minChosenInstanceId == that.minChosenInstanceId &&
                type == that.type &&
                Arrays.equals(value, that.value) &&
                Objects.equals(learnedValues, that.learnedValues);
    }

    @Override
    public String toString() {
        return "PaxosMessage{" +
                "type=" + type +
                ", instanceId=" + instanceId +
                ", nodeId=" + nodeId +
                ", toNodeId=" + toNodeId +
                ", proposalId=" + proposalId +
                ", proposalNodeId=" + proposalNodeId +
                ", value=" + Arrays.toString(value) +
                ", preAcceptId=" + preAcceptId +
                ", preAcceptNodeId=" + preAcceptNodeId +
                ", rejectByPromiseId=" + rejectByPromiseId +
                ", nowInstanceId=" + nowInstanceId +
                ", minChosenInstanceId=" + minChosenInstanceId +
                '}';
    }
}
