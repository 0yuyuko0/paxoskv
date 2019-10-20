package com.yuyuko.paxoskv.core;

import com.yuyuko.paxoskv.core.node.Config;
import com.yuyuko.paxoskv.core.storage.DataNotFoundException;
import com.yuyuko.paxoskv.core.storage.PaxosLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.yuyuko.paxoskv.core.PaxosMessage.PaxosMessageType.AcceptReply;
import static com.yuyuko.paxoskv.core.PaxosMessage.PaxosMessageType.PrepareReply;

public class Acceptor extends Base {
    private static final Logger log = LoggerFactory.getLogger(Acceptor.class);

    private PaxosLog paxosLog;

    private BallotNumber promise;

    private BallotNumber accepted;

    private byte[] acceptValue;

    public Acceptor(PaxosInstance instance, Config config, PaxosLog paxosLog) {
        super(instance, config);
        this.paxosLog = paxosLog;
    }

    public void init() {
        setInstanceId(loadNewestChosenValue());
        log.info("[Acceptor Init],promise{},accepted{}", promise, accepted);
    }

    private long loadNewestChosenValue() {
        long maxInstanceId = 0;
        try {
            maxInstanceId = paxosLog.maxInstanceId();
        } catch (DataNotFoundException ex) {
            log.info("empty storage");
            promise = new BallotNumber();
            accepted = new BallotNumber();
            return 0;
        }
        ChosenValue chosenValue = paxosLog.readChosenValue(
                maxInstanceId);
        promise = new BallotNumber(chosenValue.getPromiseId(),
                chosenValue.getPromiseNodeId());
        accepted = new BallotNumber(chosenValue.getAcceptedId(),
                chosenValue.getAcceptedNodeId());
        acceptValue = chosenValue.getAcceptedValue();
        log.info("load newest accepted proposal {}", chosenValue);
        return maxInstanceId;
    }

    public void onPrepare(PaxosMessage paxosMessage) {
        log.debug("[On Prepare] with instanceId[{}],nodeId[{}],proposalId[{}]",
                paxosMessage.getInstanceId(), paxosMessage.getNodeId(),
                paxosMessage.getProposalId());
        PaxosMessage.Builder replyMsgBuilder = PaxosMessage.builder()
                .instanceId(getInstanceId())
                .nodeId(config.getNodeId())
                .toNodeId(paxosMessage.getNodeId())
                .proposalId(paxosMessage.getProposalId())
                .type(PrepareReply);

        BallotNumber ballotNumber = new BallotNumber(paxosMessage.getProposalId(),
                paxosMessage.getNodeId());
        // 在大于当前promise值的情况下，可以接受，并且返回之前的值
        // 论文中是等于
        if (ballotNumber.compareTo(promise) >= 0) {
            log.debug("[Prepare Promise]: promiseId[{}],promiseNodeId[{}],preAccepted[{}]," +
                            "preAcceptedNodeId[{}]", ballotNumber.getProposalId(),
                    ballotNumber.getNodeId(),
                    accepted.getProposalId(), accepted.getNodeId());
            // 保存之前accept的propose id和node id
            replyMsgBuilder.preAcceptId(accepted.getProposalId())
                    .preAcceptNodeId(accepted.getNodeId());
            // 如果之前有accept的值，返回之前的数据
            if (!accepted.isEmpty()) {
                replyMsgBuilder.value(acceptValue);
            }
            // 保存这一次prepare的信息
            promise = ballotNumber;

            //持久化prepare信息
            persist();
        } else {//拒绝prepare
            log.debug("[Prepare Reject]: message proposalId[{}],nodeId[{}]. promiseId[{}]," +
                            "promiseNodeId[{}]",
                    paxosMessage.getProposalId(),
                    paxosMessage.getNodeId(),
                    promise.getProposalId(),
                    promise.getNodeId());
            replyMsgBuilder.rejectByPromiseId(promise.getProposalId());
        }

        sendMessage(replyMsgBuilder);
    }

    public void onAccept(PaxosMessage paxosMessage) {
        log.debug("[On Accept] with instanceId[{}],nodeId[{}],proposalId[{}]",
                paxosMessage.getInstanceId(), paxosMessage.getNodeId(),
                paxosMessage.getProposalId());

        PaxosMessage.Builder replyMsgBuilder = PaxosMessage.builder()
                .type(AcceptReply)
                .instanceId(instanceId)
                .nodeId(config.getNodeId())
                .toNodeId(paxosMessage.getNodeId())
                .proposalId(paxosMessage.getProposalId());

        BallotNumber ballotNumber = new BallotNumber(paxosMessage.getProposalId(),
                paxosMessage.getNodeId());
        // 在大于当前promise值的情况下，可以接受，并且返回之前的值
        // 论文中是等于
        if (ballotNumber.compareTo(promise) >= 0) {
            log.debug("[Accept Pass]: PaxosMessage proposalId[{}],nodeId[{}], My promiseId[{}], " +
                            "promiseNodeId[{}], preAccepted[{}], preAcceptedNodeId[{}]",
                    ballotNumber.getProposalId(),
                    ballotNumber.getNodeId(),
                    promise.getProposalId(),
                    promise.getNodeId(),
                    accepted.getProposalId(), accepted.getNodeId());
            promise = ballotNumber;
            accepted = ballotNumber;
            acceptValue = paxosMessage.getValue();
            persist();
        } else {
            log.debug("[Accept Reject]: message proposalId[{}],nodeId[{}]. promiseId[{}]," +
                            "promiseNodeId[{}]",
                    paxosMessage.getProposalId(),
                    paxosMessage.getNodeId(),
                    promise.getProposalId(),
                    promise.getNodeId());
            replyMsgBuilder.rejectByPromiseId(promise.getProposalId());
        }
        sendMessage(replyMsgBuilder);
    }

    private void persist() {
        ChosenValue chosenValue = new ChosenValue(instanceId, promise.getProposalId(),
                promise.getNodeId(), accepted.getProposalId(), accepted.getNodeId(), acceptValue);
        paxosLog.getUnstable().append(chosenValue);
        log.debug("[Persist] chosenValue[{}] minInstanceId[{}],maxInstanceId[{}]", chosenValue,
                paxosLog.getUnstable().minInstanceId(), paxosLog.getUnstable().maxInstanceId());
    }

    @Override
    protected void initForNewPaxosInstance() {
        accepted.reset();
        acceptValue = null;
    }

    BallotNumber getPromise() {
        return promise;
    }

    BallotNumber getAccepted() {
        return accepted;
    }

    PaxosLog getPaxosLog() {
        return paxosLog;
    }

    void setPromise(BallotNumber promise) {
        this.promise = promise;
    }

    void setAccepted(BallotNumber accepted) {
        this.accepted = accepted;
    }

    void setAcceptValue(byte[] acceptValue) {
        this.acceptValue = acceptValue;
    }

    byte[] getAcceptValue() {
        return acceptValue;
    }
}
