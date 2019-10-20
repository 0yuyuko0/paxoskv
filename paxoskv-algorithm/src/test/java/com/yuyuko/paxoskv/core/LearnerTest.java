package com.yuyuko.paxoskv.core;

import com.yuyuko.paxoskv.core.node.Config;
import com.yuyuko.paxoskv.core.node.NodeInfo;
import com.yuyuko.paxoskv.core.storage.MemoryStorage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static com.yuyuko.paxoskv.core.PaxosMessage.PaxosMessageType.AskForLearn;
import static com.yuyuko.paxoskv.core.PaxosMessage.PaxosMessageType.ProposerSendSuccess;
import static org.junit.jupiter.api.Assertions.*;

class LearnerTest {
    static final Learner newLearner(long id, List<Long> nodeIds) {
        Config config = NodeTest.newTestConfig(id,
                nodeIds.stream().map(NodeInfo::new).collect(Collectors.toList()),
                new MemoryStorage());
        PaxosInstance paxosInstance = new PaxosInstance(config);
        return paxosInstance.getLearner();
    }

    @Test
    void proposeSuccess() {
        Learner learner = newLearner(1, List.of(1L));
        learner.proposeSuccess(3, 1);
        List<PaxosMessage> messages = learner.readMessages();
        assertEquals(1, messages.size());
        assertEquals(ProposerSendSuccess, messages.get(0).getType());
    }

    @Test
    void onProposerSendSuccess() {
        Learner learner = newLearner(1, List.of(1L));
        learner.onProposerSendSuccess(
                PaxosMessage.builder().nodeId(1).proposalId(1).instanceId(3).build()
        );
        assertFalse(learner.isLearned());
        learner.onProposerSendSuccess(
                PaxosMessage.builder().nodeId(1).proposalId(1).instanceId(0).build()
        );
        assertFalse(learner.isLearned());

        learner.getAcceptor().setAccepted(new BallotNumber(1, 1));
        learner.getAcceptor().setAcceptValue("123".getBytes());
        learner.onProposerSendSuccess(
                PaxosMessage.builder().nodeId(2).proposalId(1).instanceId(0).build()
        );
        assertFalse(learner.isLearned());

        learner.onProposerSendSuccess(
                PaxosMessage.builder().nodeId(1).proposalId(1).instanceId(0).build()
        );
        assertTrue(learner.isLearned());
        assertArrayEquals("123".getBytes(), learner.getLearnedValue());
    }

    @Test
    void tick() {
        Learner learner = newLearner(1, List.of(1L));
        long timeout = learner.getAskForLearnTimeout() + 1;
        for (long i = 0; i < timeout; i++) {
            learner.tick();
        }
        List<PaxosMessage> messages = learner.readMessages();
        assertEquals(AskForLearn, messages.get(0).getType());
    }

    @Test
    void onAskForLearn() {
        Learner learner = newLearner(1, List.of(1L));
        learner.getAcceptor().getPaxosLog().maybeAppend(List.of(new ChosenValue(0),
                new ChosenValue(1)), 0);
        learner.onAskForLearn(
                PaxosMessage.builder()
                        .instanceId(0).nodeId(1).build()
        );
        assertEquals(0, learner.readMessages().size());
        learner.onAskForLearn(
                PaxosMessage.builder()
                        .instanceId(1).nodeId(2).build()
        );
        assertEquals(0, learner.readMessages().size());


        learner.setInstanceId(1);
        learner.onAskForLearn(
                PaxosMessage.builder()
                        .instanceId(0).nodeId(2).build()
        );
        List<PaxosMessage> messages = learner.readMessages();
        assertEquals(1, messages.size());
        assertEquals(2, messages.get(0).getLearnedValues().size());
        assertEquals(0, messages.get(0).getLearnedValues().get(0).getInstanceId());
    }

    @Test
    void onSendLearnValue() {
        PaxosMessage m = PaxosMessage.builder()
                .type(PaxosMessage.PaxosMessageType.SendLearnValue)
                .instanceId(1)
                .toNodeId(1)
                .nodeId(2)
                .learnedValues(List.of(new ChosenValue(1),
                        new ChosenValue(2), new ChosenValue(3))).build();
        Learner learner = newLearner(1, List.of(1L));
        assertEquals(0, learner.onSendLearnValue(m));
        assertFalse(learner.isLearned());
        learner.setInstanceId(1);
        assertEquals(3, learner.onSendLearnValue(m));
        assertTrue(learner.isLearned());
    }
}