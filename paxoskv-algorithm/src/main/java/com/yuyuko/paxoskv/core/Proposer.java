package com.yuyuko.paxoskv.core;

import com.yuyuko.paxoskv.core.node.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class Proposer extends Base {
    private static final Logger log = LoggerFactory.getLogger(Proposer.class);

    private long proposalId;

    private long highestOtherProposalId;

    private byte[] value;

    private BallotNumber highestOtherPreAcceptBallot;

    private MessageCounter messageCounter;

    private boolean isPreparing;

    private boolean isAccepting;

    private boolean canSkipPrepare;

    private long prepareElapsed;

    private long acceptElapsed;

    private long prepareTimeout;

    private long acceptTimeout;

    private long timeoutInstanceId;

    private boolean wasRejectedBySomeone;

    private final Learner learner;

    public Proposer(PaxosInstance instance, Config config, Learner learner) {
        super(instance, config);
        this.learner = learner;
        messageCounter = new MessageCounter(config);
        proposalId = 1;
        initForNewPaxosInstance();
    }

    private static class MessageCounter {
        private Config config;

        private Set<Long> receiveMsgNodeIds;

        private Set<Long> rejectMsgNodeIds;

        private Set<Long> promiseOrAcceptMsgNodeIds;

        public MessageCounter(Config config) {
            this.config = config;
            startNewRound();
        }

        public void startNewRound() {
            receiveMsgNodeIds = new HashSet<>();
            rejectMsgNodeIds = new HashSet<>();
            promiseOrAcceptMsgNodeIds = new HashSet<>();
        }

        public void addReceive(Long nodeId) {
            receiveMsgNodeIds.add(nodeId);
        }

        public void addReject(Long nodeId) {
            rejectMsgNodeIds.add(nodeId);
        }

        public void addPromiseOrAccept(Long nodeId) {
            promiseOrAcceptMsgNodeIds.add(nodeId);
        }

        public boolean passThisRound() {
            return promiseOrAcceptMsgNodeIds.size() >= config.quorum();
        }

        public boolean rejectThisRound() {
            return rejectMsgNodeIds.size() >= config.quorum();
        }

        public boolean receiveAllThisRound() {
            return receiveMsgNodeIds.size() == config.getNodeCount();
        }
    }


    @Override
    protected void initForNewPaxosInstance() {
        messageCounter.startNewRound();
        highestOtherProposalId = 0;
        value = null;
        exitAccept();
        exitPrepare();
    }

    private void exitAccept() {
        isAccepting = false;
        acceptElapsed = 0;
    }

    private void exitPrepare() {
        isPreparing = false;
        prepareElapsed = 0;
    }

    /**
     * 提议一个值
     *
     * @param value 值
     */
    public void propose(byte[] value) {
        if (this.value == null)
            this.value = value;
        //重置超时时间
        prepareTimeout = config.getPrepareTimeout();
        acceptTimeout = config.getAcceptTimeout();
        // 在可以忽略而且没有被任何一个acceptor拒绝的情况下才能直接接受
        if (canSkipPrepare && !wasRejectedBySomeone) {
            log.debug("[Skip prepare]");
            accept();
        } else
            //if not reject by someone, no need to increase ballot
            prepare(wasRejectedBySomeone);
    }

    private void accept() {
        log.debug("[Accept],proposalId:[{}],value size[{}]", proposalId, value.length);
        exitPrepare();
        isAccepting = true;
        PaxosMessage.Builder respMsgBuilder = PaxosMessage.builder()
                .type(PaxosMessage.PaxosMessageType.Accept)
                .instanceId(instanceId)
                .nodeId(config.getNodeId())
                .proposalId(proposalId)
                .value(value);
        messageCounter.startNewRound();

        addAcceptTimer(0);

        broadcastMessage(respMsgBuilder);
    }

    private void addAcceptTimer(long timeout) {
        //重置消逝的时间
        acceptElapsed = 0;
        if (timeout == 0)
            acceptTimeout = config.getAcceptTimeout();
        else
            acceptTimeout = timeout;
        timeoutInstanceId = instanceId;
    }

    public void onAcceptReply(PaxosMessage paxosMessage) {
        if (!isAccepting) {
            // 没有在接受状态
            return;
        }
        if (paxosMessage.getProposalId() != proposalId) {
            // ID不对
            return;
        }
        messageCounter.addReceive(paxosMessage.getNodeId());

        if (paxosMessage.getRejectByPromiseId() == 0) {
            //没有拒绝
            log.debug("[Accept],instanceId[{}],nodeId[{}],proposalId[{}]",
                    paxosMessage.getInstanceId(),
                    paxosMessage.getNodeId(),
                    paxosMessage.getProposalId());
            messageCounter.addPromiseOrAccept(paxosMessage.getNodeId());
        } else {
            log.debug("[Reject],instanceId[{}],nodeId[{}],proposalId[{}],rejectByPromiseId[{}]",
                    paxosMessage.getInstanceId(),
                    paxosMessage.getNodeId(),
                    paxosMessage.getProposalId(),
                    paxosMessage.getRejectByPromiseId());
            messageCounter.addReject(paxosMessage.getNodeId());
            wasRejectedBySomeone = true;
            setHighestOtherProposalId(paxosMessage.getRejectByPromiseId());
        }
        if (messageCounter.passThisRound()) {
            exitAccept();
            // 通过learn学习新提交成功的值
            learner.proposeSuccess(instanceId, proposalId);
        } else if (messageCounter.rejectThisRound() || messageCounter.receiveAllThisRound()) {
            long timeout = randomizedTimeout(config.getRestartAcceptTimeout());
            log.debug("[Accept Not Pass] wait [{}]ms and restart accept", timeout);
            addAcceptTimer(timeout);
        }
    }

    /**
     * @param timeout timeout
     * @return [0.5 * timeout,1.5 * timeout]
     */
    private static long randomizedTimeout(long timeout) {
        return timeout / 2 + ThreadLocalRandom.current().nextLong(timeout);
    }

    private void prepare(boolean needNewBallot) {
        log.debug("[Prepare] instanceId[{}],nodeId[{}],proposalId[{}],value.size[{}]",
                instanceId, config.getNodeId(), proposalId, value.length);
        //清空状态
        exitAccept();
        isPreparing = true;
        canSkipPrepare = false;
        wasRejectedBySomeone = false;
        highestOtherPreAcceptBallot = new BallotNumber();

        if (needNewBallot)
            newPrepare();
        PaxosMessage.Builder respMsgBuilder = PaxosMessage.builder()
                .type(PaxosMessage.PaxosMessageType.Prepare)
                .instanceId(instanceId)
                .nodeId(config.getNodeId())
                .proposalId(proposalId);

        messageCounter.startNewRound();

        addPrepareTimer(0);

        broadcastMessage(respMsgBuilder);
    }

    private void addPrepareTimer(long timeout) {
        //重置消逝的时间
        prepareElapsed = 0;
        if (timeout == 0)
            prepareTimeout = config.getPrepareTimeout();
        else
            prepareTimeout = timeout;
        timeoutInstanceId = instanceId;
    }

    public void onPrepareReply(PaxosMessage paxosMessage) {
        if (!isPreparing)
            return;
        if (paxosMessage.getProposalId() != proposalId) {
            // 提交的ID不对
            return;
        }
        messageCounter.addReceive(paxosMessage.getNodeId());
        if (paxosMessage.getRejectByPromiseId() == 0) {
            //没有被拒绝，保存下id与值
            BallotNumber ballotNumber = new BallotNumber(paxosMessage.getPreAcceptId(),
                    paxosMessage.getPreAcceptNodeId());
            messageCounter.addPromiseOrAccept(paxosMessage.getNodeId());
            addPreAcceptValue(ballotNumber, paxosMessage.getValue());
        } else {
            //拒绝了
            messageCounter.addReject(paxosMessage.getNodeId());
            wasRejectedBySomeone = true;
            setHighestOtherProposalId(paxosMessage.getRejectByPromiseId());
        }
        if (messageCounter.passThisRound()) {
            canSkipPrepare = true;
            log.debug("[Prepare Pass] instanceId[{}],proposalId[{}]. start accept", instanceId,
                    proposalId);
            accept();
        } else if (messageCounter.rejectThisRound() || messageCounter.receiveAllThisRound()) {
            long timeout = randomizedTimeout(config.getRestartPrepareTimeout());
            log.debug("[Prepare Not Pass] wait [{}]ms and restart prepare", timeout);
            addPrepareTimer(timeout);
        }
    }

    private void addPreAcceptValue(BallotNumber preAcceptBallot, byte[] preAcceptValue) {
        log.debug("[Add PreAccept]. preAcceptId[{}],preAcceptNodeId[{}]," +
                        "highestOtherPreAcceptId[{}]," +
                        "highestOtherPreAcceptNodeId[{}],", preAcceptBallot.getProposalId(),
                preAcceptBallot.getProposalId(), highestOtherPreAcceptBallot.getProposalId(),
                highestOtherPreAcceptBallot.getNodeId());
        if (preAcceptBallot.isEmpty())
            return;
        if (preAcceptBallot.compareTo(highestOtherPreAcceptBallot) >= 0) {
            highestOtherPreAcceptBallot = preAcceptBallot;
            value = preAcceptValue;
        }
    }

    private void newPrepare() {
        log.debug("[New Prepare],proposalId[{}],highestOther[{}],nodeId[{}]", proposalId,
                highestOtherProposalId, config.getNodeId());
        proposalId = Math.max(proposalId, highestOtherProposalId) + 1;
    }

    public void tick() {
        if (isPreparing) {
            ++prepareElapsed;
            if (instanceId != timeoutInstanceId) {
                log.debug("timeoutInstanceId {} not same to nowInstanceId {}", timeoutInstanceId,
                        instanceId);
                prepareElapsed = 0;
                return;
            }
            if (prepareElapsed == prepareTimeout) {
                prepareElapsed = 0;
                log.debug("[Restart Prepare] when preparing");
                prepare(wasRejectedBySomeone);
            }
        } else if (isAccepting) {
            ++acceptElapsed;
            if (instanceId != timeoutInstanceId) {
                log.debug("timeoutInstanceId {} not same to nowInstanceId {}", timeoutInstanceId,
                        instanceId);
                acceptElapsed = 0;
                return;
            }
            if (acceptElapsed == acceptTimeout) {
                acceptElapsed = 0;
                log.debug("[Restart Prepare] when accepting");
                prepare(wasRejectedBySomeone);
            }
        }
    }

    public void setProposalId(long proposalId) {
        this.proposalId = proposalId;
    }

    public long getPrepareTimeout() {
        return prepareTimeout;
    }

    public long getAcceptTimeout() {
        return acceptTimeout;
    }

    public long getProposalId() {
        return proposalId;
    }

    byte[] getValue() {
        return value;
    }

    boolean isPreparing() {
        return isPreparing;
    }

    boolean isAccepting() {
        return isAccepting;
    }

    void setCanSkipPrepare(boolean canSkipPrepare) {
        this.canSkipPrepare = canSkipPrepare;
    }

    void setWasRejectedBySomeone(boolean wasRejectedBySomeone) {
        this.wasRejectedBySomeone = wasRejectedBySomeone;
    }


    void setHighestOtherProposalId(long otherProposalId) {
        if (otherProposalId > highestOtherProposalId)
            highestOtherProposalId = otherProposalId;
    }

    long getHighestOtherProposalId() {
        return highestOtherProposalId;
    }

    public Learner getLearner() {
        return learner;
    }

    public boolean isWasRejectedBySomeone() {
        return wasRejectedBySomeone;
    }
}
