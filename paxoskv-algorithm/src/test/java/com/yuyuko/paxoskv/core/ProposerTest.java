package com.yuyuko.paxoskv.core;

import com.yuyuko.paxoskv.core.node.Config;
import com.yuyuko.paxoskv.core.node.NodeInfo;
import com.yuyuko.paxoskv.core.storage.MemoryStorage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static com.yuyuko.paxoskv.core.PaxosMessage.PaxosMessageType.*;
import static org.junit.jupiter.api.Assertions.*;

class ProposerTest {
    static final Proposer newProposer(long id, List<Long> nodeIds) {
        Config config = NodeTest.newTestConfig(id,
                nodeIds.stream().map(NodeInfo::new).collect(Collectors.toList()),
                new MemoryStorage());
        PaxosInstance paxosInstance = new PaxosInstance(config);
        return paxosInstance.getProposer();
    }

    @Test
    void propose() {
        Proposer proposer = newProposer(1, List.of(1L));
        proposer.propose("123".getBytes());
        assertTrue(proposer.isPreparing());
        assertArrayEquals("123".getBytes(), proposer.getValue());
        List<PaxosMessage> messages = proposer.readMessages();
        assertEquals(1, messages.size());
        assertEquals(PaxosMessage.PaxosMessageType.Prepare, messages.get(0).getType());
    }

    @Test
    void proposeSkipPrepare() {
        Proposer proposer = newProposer(1, List.of(1L));
        proposer.setCanSkipPrepare(true);
        proposer.propose("123".getBytes());
        assertTrue(proposer.isAccepting());
        assertArrayEquals("123".getBytes(), proposer.getValue());
        List<PaxosMessage> messages = proposer.readMessages();
        assertEquals(1, messages.size());
        assertEquals(PaxosMessage.PaxosMessageType.Accept, messages.get(0).getType());
    }

    @Test
    void proposeRejectBySomeone() {
        Proposer proposer = newProposer(1, List.of(1L));
        proposer.setWasRejectedBySomeone(true);
        proposer.setHighestOtherProposalId(5);
        proposer.propose("123".getBytes());
        assertTrue(proposer.isPreparing());
        assertEquals(6, proposer.getProposalId());
        assertArrayEquals("123".getBytes(), proposer.getValue());
        List<PaxosMessage> messages = proposer.readMessages();
        assertEquals(1, messages.size());
        assertEquals(PaxosMessage.PaxosMessageType.Prepare, messages.get(0).getType());
    }

    @Test
    void onAcceptReply() {
        Proposer proposer = newProposer(1, List.of(1L));
        proposer.setCanSkipPrepare(true);
        proposer.propose("123".getBytes());
        List<PaxosMessage> messages = proposer.readMessages();
        assertEquals(1, messages.size());
        assertEquals(PaxosMessage.PaxosMessageType.Accept, messages.get(0).getType());
        proposer.onAcceptReply(PaxosMessage.builder(messages.get(0)).type(AcceptReply).build());
        assertFalse(proposer.isAccepting());
        List<PaxosMessage> messages1 = proposer.getLearner().readMessages();
        assertEquals(1, messages1.size());
        assertEquals(ProposerSendSuccess, messages1.get(0).getType());
    }

    @Test
    void onAcceptRejectReply() {
        Proposer proposer = newProposer(1, List.of(1L));
        proposer.setCanSkipPrepare(true);
        proposer.propose("123".getBytes());
        List<PaxosMessage> messages = proposer.readMessages();
        assertEquals(1, messages.size());
        assertEquals(PaxosMessage.PaxosMessageType.Accept, messages.get(0).getType());
        proposer.onAcceptReply(PaxosMessage.builder(messages.get(0)).rejectByPromiseId(5L)
                .type(AcceptReply).build());
        assertTrue(proposer.isAccepting());
        assertTrue(proposer.isWasRejectedBySomeone());
        assertEquals(5, proposer.getHighestOtherProposalId());
    }

    @Test
    void onPrepareReply() {
        Proposer proposer = newProposer(1, List.of(1L));
        proposer.propose("123".getBytes());
        List<PaxosMessage> messages = proposer.readMessages();
        assertEquals(1, messages.size());
        proposer.onPrepareReply(messages.get(0));
        List<PaxosMessage> messages1 = proposer.readMessages();
        assertTrue(proposer.isAccepting());
        assertEquals(PaxosMessage.PaxosMessageType.Accept, messages1.get(0).getType());
    }

    @Test
    void onPrepareReplyReject() {
        Proposer proposer = newProposer(1, List.of(1L));
        proposer.propose("123".getBytes());
        List<PaxosMessage> messages = proposer.readMessages();
        assertEquals(1, messages.size());
        proposer.onPrepareReply(PaxosMessage.builder(messages.get(0)).rejectByPromiseId(5).build());
        assertTrue(proposer.isPreparing());
        assertEquals(5, proposer.getHighestOtherProposalId());
    }

    @Test
    void tick() {
        Proposer proposer = newProposer(1, List.of(1L));
        proposer.propose("123".getBytes());
        proposer.readMessages();
        for (long i = 0; i < proposer.getPrepareTimeout() + 1; i++) {
            proposer.tick();
        }
        List<PaxosMessage> messages = proposer.readMessages();
        assertEquals(1, messages.size());
        assertEquals(PaxosMessage.PaxosMessageType.Prepare, messages.get(0).getType());

        proposer.setCanSkipPrepare(true);
        proposer.propose("123".getBytes());
        proposer.readMessages();
        for (long i = 0; i < proposer.getAcceptTimeout() + 1; i++) {
            proposer.tick();
        }
        List<PaxosMessage> messages1 = proposer.readMessages();
        assertEquals(1, messages1.size());
        assertEquals(Prepare, messages1.get(0).getType());
    }
}