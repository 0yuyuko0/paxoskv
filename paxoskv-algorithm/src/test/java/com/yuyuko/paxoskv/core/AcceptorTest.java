package com.yuyuko.paxoskv.core;

import com.yuyuko.paxoskv.core.node.Config;
import com.yuyuko.paxoskv.core.node.NodeInfo;
import com.yuyuko.paxoskv.core.storage.MemoryStorage;
import com.yuyuko.paxoskv.core.storage.PaxosLog;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static com.yuyuko.paxoskv.core.PaxosMessage.PaxosMessageType.*;
import static org.junit.jupiter.api.Assertions.*;

class AcceptorTest {

    @Test
    void init() {
        Acceptor acceptor = new Acceptor(null, null, new PaxosLog(new MemoryStorage()));
        acceptor.init();
        assertEquals(0, acceptor.getInstanceId());
        assertEquals(new BallotNumber(0, 0), acceptor.getPromise());
        assertEquals(new BallotNumber(0, 0), acceptor.getAccepted());
        MemoryStorage storage = new MemoryStorage();
        PaxosLog log = new PaxosLog(storage);
        storage.append(List.of(new ChosenValue(0, 1, 1, 1, 1, "123".getBytes())));


        Acceptor acceptor1 = new Acceptor(null, null, log);
        acceptor1.init();
        assertEquals(0, acceptor1.getInstanceId());
        assertEquals(new BallotNumber(1, 1), acceptor1.getPromise());
        assertEquals(new BallotNumber(1, 1), acceptor1.getAccepted());
        assertArrayEquals("123".getBytes(), acceptor1.getAcceptValue());
    }

    static final Acceptor newAcceptor(long id, List<Long> nodeIds) {
        Config config = NodeTest.newTestConfig(id,
                nodeIds.stream().map(NodeInfo::new).collect(Collectors.toList()),
                new MemoryStorage());
        PaxosInstance paxosInstance = new PaxosInstance(config);
        return paxosInstance.getAcceptor();
    }

    @Test
    void onPrepare() {
        Acceptor acceptor = newAcceptor(1, List.of(1L));
        acceptor.onPrepare(
                PaxosMessage.builder().type(Prepare)
                        .proposalId(1).instanceId(0).nodeId(1).build()
        );
        List<PaxosMessage> messages = acceptor.readMessages();
        assertEquals(new BallotNumber(1, 1), acceptor.getPromise());
        assertEquals(1, messages.size());
        assertEquals(PrepareReply, messages.get(0).getType());
        assertEquals(0, acceptor.getPaxosLog().getUnstable().maxInstanceId());
    }

    @Test
    void onPrepareReject() {
        Acceptor acceptor = newAcceptor(1, List.of(1L));
        acceptor.setPromise(new BallotNumber(1, 2));
        acceptor.onPrepare(
                PaxosMessage.builder().type(Prepare)
                        .proposalId(1).instanceId(0).nodeId(1).build()
        );
        List<PaxosMessage> messages = acceptor.readMessages();
        assertEquals(new BallotNumber(1, 2), acceptor.getPromise());
        assertEquals(1, messages.size());
        assertEquals(PrepareReply, messages.get(0).getType());
        assertEquals(1, messages.get(0).getRejectByPromiseId());
        assertThrows(Throwable.class, () -> acceptor.getPaxosLog().getUnstable().maxInstanceId());
    }

    @Test
    void onAccept() {
        Acceptor acceptor = newAcceptor(1, List.of(1L));
        acceptor.onAccept(
                PaxosMessage.builder().type(Accept)
                        .proposalId(1).instanceId(0).nodeId(1)
                        .value("123".getBytes())
                        .build()
        );
        List<PaxosMessage> messages = acceptor.readMessages();
        assertEquals(1, messages.size());
        assertEquals(AcceptReply, messages.get(0).getType());
        assertEquals(1, acceptor.getPaxosLog().getUnstable().lastChosenValue().getAcceptedNodeId());
        assertArrayEquals("123".getBytes(),
                acceptor.getPaxosLog().getUnstable().lastChosenValue().getAcceptedValue());
    }

    @Test
    void onAcceptReject() {
        Acceptor acceptor = newAcceptor(1, List.of(1L));
        acceptor.setPromise(new BallotNumber(1, 2));
        acceptor.onAccept(
                PaxosMessage.builder().type(Accept)
                        .proposalId(1).instanceId(0).nodeId(1)
                        .value("123".getBytes())
                        .build()
        );
        List<PaxosMessage> messages = acceptor.readMessages();
        assertEquals(1, messages.size());
        assertEquals(AcceptReply, messages.get(0).getType());
        assertTrue(acceptor.getPaxosLog().getUnstable().isEmpty());
        assertEquals(1, messages.get(0).getRejectByPromiseId());
    }
}