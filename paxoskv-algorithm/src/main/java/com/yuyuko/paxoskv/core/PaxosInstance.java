package com.yuyuko.paxoskv.core;

import com.yuyuko.paxoskv.core.node.Config;
import com.yuyuko.paxoskv.core.storage.PaxosLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PaxosInstance {
    private static final Logger log = LoggerFactory.getLogger(PaxosInstance.class);

    private final Config config;

    private final Acceptor acceptor;

    private final Proposer proposer;

    private final Learner learner;

    private final Committer committer;

    private final CommitContext commitContext;

    private final PaxosLog paxosLog;

    public PaxosInstance(Config config) {

        this.config = config;
        this.paxosLog = new PaxosLog(config.getStorage());
        this.acceptor = new Acceptor(this, config, paxosLog);
        this.learner = new Learner(this, config, acceptor, paxosLog);
        this.proposer = new Proposer(this, config, learner);
        this.commitContext = new CommitContext();
        this.committer = new Committer(commitContext);

        init();
    }


    public void propose(byte[] value) {
        if (!commitContext.isNewCommit())
            return;
        if (!learner.hasLatestInstanceId())
            return;
        commitContext.startCommit(proposer.getInstanceId());
        proposer.propose(value);
    }

    public void tick() {
        proposer.tick();
        learner.tick();
    }

    public void step(PaxosMessage m) {
        log.trace("[Step PaxosMessage],Now instanceId {},msg instanceId {},msg type {}, my node id {}," +
                        "from node id {} ,lastestInstanceId {}", proposer.getInstanceId(),
                m.getInstanceId(), m.getType(), config.getNodeId(),
                m.getNodeId(), learner.getSeenHighestInstanceId());
        switch (m.getType()) {
            case Prepare:
            case Accept:
                stepAcceptor(m);
                break;
            case PrepareReply:
            case AcceptReply:
                stepProposer(m);
                break;
            case AskForLearn:
            case SendLearnValue:
            case ProposerSendSuccess:
                stepLearner(m);
                break;
        }
    }

    private void stepProposer(PaxosMessage m) {
        if (m.getInstanceId() != proposer.getInstanceId())
            return;
        if (m.getType() == PaxosMessage.PaxosMessageType.PrepareReply)
            proposer.onPrepareReply(m);
        else if (m.getType() == PaxosMessage.PaxosMessageType.AcceptReply)
            proposer.onAcceptReply(m);
    }

    private void stepAcceptor(PaxosMessage m) {
        // 消息的实例ID正好是当前acceptor的实例ID+1
        // 表示这个消息已经被投票通过了
        if (m.getInstanceId() == acceptor.getInstanceId() + 1) {
            stepLearner(
                    PaxosMessage.builder(m)
                            .instanceId(acceptor.getInstanceId())
                            .type(PaxosMessage.PaxosMessageType.ProposerSendSuccess)
                            .build()
            );
        }// 消息的实例ID正好是当前acceptor的实例ID
        // 表示是正在进行投票的消息
        else if (m.getInstanceId() == acceptor.getInstanceId()) {
            switch (m.getType()) {
                case Prepare:
                    acceptor.onPrepare(m);
                    break;
                case Accept:
                    acceptor.onAccept(m);
                    break;
            }
        }
    }

    void newInstance() {
        acceptor.newInstance();
        learner.newInstance();
        proposer.newInstance();
    }

    private void stepLearner(PaxosMessage m) {
        long maxInstanceId = 0;
        switch (m.getType()) {
            case AskForLearn:
                learner.onAskForLearn(m);
                break;
            case SendLearnValue:
                maxInstanceId = learner.onSendLearnValue(m);
                break;
            case ProposerSendSuccess:
                learner.onProposerSendSuccess(m);
                break;
        }
        if (learner.isLearned()) {
            commitContext.setResult(CommitContext.CommitResult.OK, learner.getInstanceId(),
                    learner.getLearnedValue());
            if (m.getType() == PaxosMessage.PaxosMessageType.SendLearnValue) {
                //如果获得了学习了多个值
                if (learner.getInstanceId() <= maxInstanceId) {
                    while (learner.getInstanceId() <= maxInstanceId)
                        newInstance();
                    log.info("[Learned] New paxos starting Node Id {},Proposer InstanceId {}," +
                                    "Acceptor InstanceId {}, Learner InstanceId {}",
                            config.getNodeId(),
                            proposer.getInstanceId(),
                            acceptor.getInstanceId(),
                            learner.getInstanceId());
                }
            } else {
                newInstance();
                log.info("[Learned] New paxos starting,Node Id {},Proposer InstanceId {}," +
                                "Acceptor" +
                                " InstanceId {},Learner InstanceId {}",
                        config.getNodeId(),
                        proposer.getInstanceId(),
                        acceptor.getInstanceId(),
                        learner.getInstanceId());
            }
        }
    }

    public void init() {
        //Must init acceptor first, because the max instanceid is record in acceptor state.
        acceptor.init();
        learner.setInstanceId(acceptor.getInstanceId());
        proposer.setInstanceId(acceptor.getInstanceId());
        // proposer的下一个提案ID从acceptor已经承诺的ID+1开始
        proposer.setProposalId(acceptor.getPromise().getProposalId() + 1);
    }

    public Committer getCommitter() {
        return committer;
    }

    public Acceptor getAcceptor() {
        return acceptor;
    }

    public Proposer getProposer() {
        return proposer;
    }

    public Learner getLearner() {
        return learner;
    }

    public Config getConfig() {
        return config;
    }

    public long nowInstanceId() {
        return acceptor.getInstanceId();
    }

    public PaxosLog getPaxosLog() {
        return paxosLog;
    }

    List<PaxosMessage> readMessages() {
        List<PaxosMessage> messages = new ArrayList<>();
        messages.addAll(proposer.readMessages());
        messages.addAll(acceptor.readMessages());
        messages.addAll(learner.readMessages());
        return messages;
    }

    public List<PaxosMessage> getMessages() {
        List<PaxosMessage> messages = new ArrayList<>();
        messages.addAll(proposer.getMessages());
        messages.addAll(acceptor.getMessages());
        messages.addAll(learner.getMessages());
        return messages;
    }

    public void clearMessages() {
        proposer.clearMessages();
        acceptor.clearMessages();
        learner.clearMessages();
    }
}
