package com.yuyuko.paxoskv.core;

/**
 * 封装了nodeid与proposeid的类
 */
public class BallotNumber implements Comparable<BallotNumber> {
    private long proposalId;

    private long nodeId;

    public BallotNumber(long proposalId, long nodeId) {
        this.proposalId = proposalId;
        this.nodeId = nodeId;
    }

    public BallotNumber() {

    }


    public boolean isEmpty() {
        return proposalId == 0;
    }

    public void reset() {
        proposalId = nodeId = 0;
    }

    public int compareTo(BallotNumber o) {
        return proposalId == o.proposalId ?
                Long.compare(nodeId, o.nodeId) : Long.compare(proposalId, o.proposalId);
    }

    public long getProposalId() {
        return proposalId;
    }

    public long getNodeId() {
        return nodeId;
    }

    @Override
    public String toString() {
        return "BallotNumber{" +
                "proposalId=" + proposalId +
                ", nodeId=" + nodeId +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BallotNumber that = (BallotNumber) o;
        return proposalId == that.proposalId &&
                nodeId == that.nodeId;
    }
}
