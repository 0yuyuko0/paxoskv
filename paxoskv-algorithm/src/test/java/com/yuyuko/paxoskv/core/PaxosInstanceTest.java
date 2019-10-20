package com.yuyuko.paxoskv.core;

import com.yuyuko.paxoskv.core.node.Config;
import com.yuyuko.paxoskv.core.node.NodeInfo;
import com.yuyuko.paxoskv.core.storage.MemoryStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.yuyuko.paxoskv.core.PaxosMessage.PaxosMessageType.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.of;

class PaxosInstanceTest {
    public static final PaxosInstance newPaxosInstanceSingleNode() {
        Config config = new Config();
        config.setNodeId(1L);
        config.setNodeCount(1);
        config.setNodeInfoList(List.of(new NodeInfo(1)));
        config.setStorage(new MemoryStorage());
        return new PaxosInstance(config);
    }

    @Test
    void tick() {
        PaxosInstance paxosInstance = newPaxosInstanceSingleNode();

        paxosInstance.getProposer().propose("123".getBytes());

        paxosInstance.getProposer().readMessages();

        long maxTick = Math.max(paxosInstance.getLearner().getAskForLearnTimeout(),
                paxosInstance.getProposer().getAcceptTimeout());
        for (long i = 0; i < maxTick; i++) {
            paxosInstance.tick();
        }
        List<PaxosMessage> messages = paxosInstance.readMessages();
        assertTrue(messages.stream().anyMatch(m -> m.getType() == AskForLearn));
        assertTrue(messages.stream().anyMatch(m -> m.getType() == Prepare));
    }

    @ParameterizedTest
    @MethodSource("stepGen")
    void step(PaxosMessage m, Consumer<PaxosInstance> consumer) {
        PaxosInstance paxosInstance = newPaxosInstanceSingleNode();
        paxosInstance.getAcceptor().setAccepted(new BallotNumber(1, 1));
        paxosInstance.step(m);
        assertDoesNotThrow(() -> consumer.accept(paxosInstance));
    }

    static Stream<Arguments> stepGen() {
        return Stream.of(
                of(
                        PaxosMessage.builder()
                                .type(PaxosMessage.PaxosMessageType.Prepare)
                                .instanceId(0)
                                .nodeId(1)
                                .proposalId(1).build(),
                        (Consumer<PaxosInstance>) paxosInstance -> {
                            List<PaxosMessage> messages = paxosInstance.readMessages();
                            assertEquals(1, messages.size());
                            assertSame(messages.get(0).getType(), PrepareReply);
                        }
                ),
                of(
                        PaxosMessage.builder()
                                .type(PaxosMessage.PaxosMessageType.Accept)
                                .instanceId(0)
                                .nodeId(1)
                                .proposalId(1)
                                .value("123".getBytes())
                                .build(),
                        (Consumer<PaxosInstance>) paxosInstance -> {
                            List<PaxosMessage> messages = paxosInstance.readMessages();
                            assertEquals(1, messages.size());
                            assertSame(messages.get(0).getType(), AcceptReply);
                        }
                ),
                of(
                        PaxosMessage.builder()
                                .type(PaxosMessage.PaxosMessageType.SendLearnValue)
                                .instanceId(0)
                                .nodeId(1)
                                .learnedValues(List.of(new ChosenValue(0),
                                        new ChosenValue(1), new ChosenValue(2))).build(),
                        (Consumer<PaxosInstance>) paxosInstance -> {
                            assertEquals(3, paxosInstance.nowInstanceId());
                        }
                ),
                of(
                        PaxosMessage.builder()
                                .type(ProposerSendSuccess)
                                .instanceId(0)
                                .nodeId(1)
                                .proposalId(1)
                                .build(),
                        (Consumer<PaxosInstance>) paxosInstance -> {
                            assertEquals(1, paxosInstance.nowInstanceId());
                        }
                )
        );
    }

    @Test
    void init() {
        PaxosInstance paxosInstance = newPaxosInstanceSingleNode();
        assertEquals(0, paxosInstance.getLearner().getInstanceId());
        assertEquals(0, paxosInstance.getProposer().getInstanceId());
        assertEquals(1, paxosInstance.getProposer().getProposalId());
        assertEquals(0, paxosInstance.getAcceptor().getInstanceId());
    }
}