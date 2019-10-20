package com.yuyuko.paxoskv.core;

import com.yuyuko.paxoskv.core.node.Config;
import com.yuyuko.paxoskv.core.storage.DataNotFoundException;
import com.yuyuko.paxoskv.core.storage.PaxosLog;
import com.yuyuko.paxoskv.core.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Learner extends Base {
    private static final Logger log = LoggerFactory.getLogger(Learner.class);

    private long highestSeenInstanceId;

    private byte[] learnedValue;

    private boolean isLearned;

    private long askForLearnTimeout;

    private long askForLearnElapsed;


    private final PaxosLog paxosLog;

    private final Acceptor acceptor;

    public Learner(PaxosInstance instance, Config config, Acceptor acceptor, PaxosLog paxosLog) {
        super(instance, config);
        this.acceptor = acceptor;
        this.paxosLog = paxosLog;
        resetAskForLearn();
    }

    @Override
    protected void initForNewPaxosInstance() {
        learnedValue = null;
        isLearned = false;
    }

    /**
     * 提议通过时调用关掉函数
     */
    public void proposeSuccess(long instanceId, long proposalId) {
        PaxosMessage.Builder builder = PaxosMessage.builder()
                .type(PaxosMessage.PaxosMessageType.ProposerSendSuccess)
                .instanceId(instanceId)
                .proposalId(proposalId)
                .nodeId(config.getNodeId());

        // 向其他参与者广播新学习到的值
        broadcastMessage(builder);
    }

    public void onProposerSendSuccess(PaxosMessage paxosMessage) {
        if (paxosMessage.getInstanceId() != instanceId) {
            log.debug("[Skip Learn Msg] instanceId not same");
            return;
        }
        if (acceptor.getAccepted().isEmpty()) {
            log.debug("[Skip Learn Msg] not accepted");
            return;
        }

        BallotNumber ballotNumber = new BallotNumber(paxosMessage.getProposalId(),
                paxosMessage.getNodeId());

        if (!acceptor.getAccepted().equals(ballotNumber)) {
            //提议id不一样，不是chosen value
            log.debug("[Skip Learn Msg] proposalBallot not same to acceptedBallot ");
            return;
        }

        //learn value.
        // 因为这里是proposer提交成功之后调用该函数
        // 所以是调用LearnValueWithoutWrite，因为在提交成功
        // 时已经调用acceptor保存了数据
        learnValue(acceptor.getAcceptValue());
    }

    public void tick() {
        ++askForLearnElapsed;
        if (askForLearnElapsed == askForLearnTimeout) {
            askForLearn();
            resetAskForLearn();
        }
    }

    private void resetAskForLearn() {
        askForLearnElapsed = 0;
        askForLearnTimeout =
                config.getAskForLearnTimeout() / 2 + ThreadLocalRandom.current().nextLong(config.getAskForLearnTimeout());
    }

    private void learnValue(byte[] value) {
        learnedValue = value;
        isLearned = true;
    }

    public long getSeenHighestInstanceId() {
        return highestSeenInstanceId;
    }

    private void setSeenHighestInstanceId(long instanceId, long nodeId) {
        if (instanceId > highestSeenInstanceId) {
            highestSeenInstanceId = instanceId;
        }
    }

    private void askForLearn() {
        PaxosMessage.Builder builder = PaxosMessage.builder()
                .instanceId(instanceId)
                .nodeId(config.getNodeId())
                .type(PaxosMessage.PaxosMessageType.AskForLearn);
        broadcastMessage(builder);
    }


    public void onAskForLearn(PaxosMessage m) {
        if (m.getNodeId() == config.getNodeId())
            return;
        setSeenHighestInstanceId(m.getInstanceId(), m.getNodeId());
        // 实例ID比自己还大，此时不用同步数据
        if (m.getInstanceId() >= instanceId) {
            return;
        }
        List<ChosenValue> learnValues;
        try {
            learnValues = paxosLog.listAcceptedValuesFrom(m.getInstanceId());
        } catch (DataNotFoundException ex) {
            log.debug("[Ask For Learn Data Not Found] from instanceId {}", m.getInstanceId());
            return;
        }
        if (learnValues.size() > 0)
            sendLearnValue(m.getNodeId(), m.getInstanceId(), learnValues);
    }

    private void sendLearnValue(long toNodeId, long learnInstanceId,
                                List<ChosenValue> chosenValues) {
        log.info("[Send Learn Value] toNodeId[{}],fromInstanceId[{}],length[{}]", toNodeId,
                learnInstanceId, chosenValues.size());
        PaxosMessage.Builder builder = PaxosMessage.builder()
                .type(PaxosMessage.PaxosMessageType.SendLearnValue)
                .instanceId(learnInstanceId)
                .toNodeId(toNodeId)
                .nodeId(config.getNodeId())
                .learnedValues(chosenValues);
        sendMessage(builder);
    }

    /**
     * @param m 消息
     * @return 学习到的最大的instanceId
     */
    public long onSendLearnValue(PaxosMessage m) {
        if (m.getInstanceId() > instanceId) {
            log.info("[Can't Learn] msg with instanceId[{}] greater than now instanceId[{}]",
                    m.getInstanceId(), instance);
            return instanceId;
        }

        if (Utils.isEmpty(m.getLearnedValues()))
            return instanceId;

        ChosenValue maxLearnValue = paxosLog.maybeAppend(m.getLearnedValues(), instanceId);

        if (maxLearnValue.getInstanceId() >= instanceId) {
            log.info("[Learn Success], learn from node[{}], learn from instanceId[{}] to " +
                    "instanceId[{}]", m.getNodeId(), instanceId, maxLearnValue.getInstanceId());
            learnValue(maxLearnValue.getAcceptedValue());
        } else {
            log.debug("[Learn Ignore], ignore learnValues from node[{}]", m.getNodeId());
        }

        return maxLearnValue.getInstanceId();
    }

    public boolean isLearned() {
        return isLearned;
    }

    public byte[] getLearnedValue() {
        return learnedValue;
    }

    public long getAskForLearnTimeout() {
        return askForLearnTimeout;
    }

    public boolean hasLatestInstanceId() {
        return instanceId + 1 >= highestSeenInstanceId;
    }

    Acceptor getAcceptor() {
        return acceptor;
    }
}
