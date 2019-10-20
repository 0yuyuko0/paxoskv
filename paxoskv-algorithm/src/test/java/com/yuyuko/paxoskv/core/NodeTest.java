package com.yuyuko.paxoskv.core;

import com.yuyuko.paxoskv.core.node.*;
import com.yuyuko.paxoskv.core.storage.MemoryStorage;
import com.yuyuko.paxoskv.core.storage.Storage;
import com.yuyuko.paxoskv.core.utils.Tuple;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.yuyuko.paxoskv.core.PaxosMessage.PaxosMessageType.Accept;
import static org.junit.jupiter.api.Assertions.*;

public class NodeTest {
    static DefaultNode newSingleNode(Storage storage) {
        return ((DefaultNode) DefaultNode.startNode(newTestConfig(1, List.of(new NodeInfo(1L)),
                storage)));
    }

    static DefaultNode newNode(long id, List<NodeInfo> peers, Storage storage) {
        return DefaultNode.startNode(newTestConfig(id, peers, storage));
    }

    public static Config newTestConfig(long id, List<NodeInfo> peers, Storage storage) {
        Config config = new Config();
        config.setNodeId(id);
        config.setNodeCount(peers.size());
        config.setNodeInfoList(peers);
        config.setStorage(storage);
        config.setAskForLearnTimeout(3);
        config.setAcceptTimeout(6);
        config.setPrepareTimeout(6);
        config.setRestartPrepareTimeout(3);
        config.setRestartAcceptTimeout(3);
        return config;
    }

    private static void sleep(long milis) {
        try {
            Thread.sleep(milis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static class Network {
        private Map<Long, DefaultNode> peers;
        private Map<Long, MemoryStorage> storages;
        private Set<Tuple<Long, Long>> dropMsg = new HashSet<>();
        private EnumSet<PaxosMessage.PaxosMessageType> ignoreMsgTypes =
                EnumSet.noneOf(PaxosMessage.PaxosMessageType.class);
        private ExecutorService executors = Executors.newFixedThreadPool(9);

        public Network(Map<Long, DefaultNode> peers, Map<Long, MemoryStorage> storages) {
            this.peers = peers;
            this.storages = storages;
        }

        public void propose(long nodeId, byte[] value) throws InterruptedException {
            DefaultNode node = peers.get(nodeId);
            AtomicBoolean finished = new AtomicBoolean();
            CountDownLatch latch = new CountDownLatch(1);
            executors.execute(
                    () -> {
                        latch.countDown();
                        node.propose(value);
                        finished.set(true);
                    }
            );
            latch.await();
            TimeUnit.MILLISECONDS.sleep(100);
            LinkedList<PaxosMessage> messages =
                    new LinkedList<>(filter(node.getPaxosInstance().readMessages()));
            while (!messages.isEmpty()) {
                PaxosMessage m = messages.removeFirst();
                PaxosInstance instance = peers.get(m.getToNodeId()).getPaxosInstance();
                instance.step(m);
                messages.addAll(filter(instance.readMessages()));
            }
        }

        public void isolate(long id) {
            for (Long nid : peers.keySet()) {
                if (id != nid)
                    cut(id, nid);
            }
        }

        public void ignore(PaxosMessage.PaxosMessageType type) {
            ignoreMsgTypes.add(type);
        }

        public void recover() {
            dropMsg.clear();
            ignoreMsgTypes.clear();
        }

        public void cancelIgnore(PaxosMessage.PaxosMessageType type) {
            ignoreMsgTypes.remove(type);
        }

        public void cut(long one, long other) {
            drop(one, other);
            drop(other, one);
        }

        private List<PaxosMessage> filter(List<PaxosMessage> messages) {
            return messages.stream().filter(
                    m -> !dropMsg.contains(new Tuple<>(m.getNodeId(), m.getToNodeId()))
                            && !ignoreMsgTypes.contains(m.getType())
            ).collect(Collectors.toList());
        }


        private void drop(long from, long to) {
            dropMsg.add(new Tuple<>(from, to));
        }

        public void tickAskForLearn(long nodeId) {
            PaxosInstance paxosInstance = peers.get(nodeId).getPaxosInstance();
            Learner learner = paxosInstance.getLearner();
            for (long i = 0; i < learner.getAskForLearnTimeout() + 1; i++) {
                learner.tick();
            }
            LinkedList<PaxosMessage> messages = new LinkedList<>(filter(learner.readMessages()));
            while (!messages.isEmpty()) {
                PaxosMessage m = messages.removeFirst();
                PaxosInstance instance = peers.get(m.getToNodeId()).getPaxosInstance();
                instance.step(m);
                messages.addAll(filter(instance.readMessages()));
            }
        }

        public PaxosInstance getInstance(long nodeId) {
            return peers.get(nodeId).getPaxosInstance();
        }
    }

    static Network newNetwork(int nodeCount) {
        List<NodeInfo> nodeInfos =
                IntStream.rangeClosed(1, nodeCount).boxed().map(NodeInfo::new).collect(Collectors.toList());
        Map<Long, DefaultNode> peers = new HashMap<>();
        Map<Long, MemoryStorage> storages = new HashMap<>();
        for (int i = 1; i <= nodeCount; i++) {
            MemoryStorage storage = new MemoryStorage();
            DefaultNode node = newNode(i, nodeInfos, storage);
            peers.put(((long) i), node);
            storages.put((long) i, storage);
        }
        return new Network(peers, storages);
    }

    @Test
    public void start() {
        DefaultNode node = newSingleNode(new MemoryStorage());
        assertEquals(0, node.getPaxosInstance().nowInstanceId());
    }

    @Test
    public void tick() {
        DefaultNode node = newSingleNode(new MemoryStorage());
        Config config = node.getPaxosInstance().getConfig();
        long acceptTimeout = config.getAcceptTimeout() + 1;
        for (long i = 0; i < acceptTimeout; i++) {
            node.tick();
        }
        sleep(100);
        Ready rd = node.ready().read();
        List<PaxosMessage> messages = rd.getMessages();
        assertTrue(messages.stream().anyMatch(m -> m.getType() == PaxosMessage.PaxosMessageType.AskForLearn));
    }

    @Test
    public void propose() throws InterruptedException {
        Network network = newNetwork(3);
        network.propose(1, "123".getBytes());
        network.propose(1, "456".getBytes());
        network.propose(1, "789".getBytes());
    }

    @Test
    public void benchmark() throws InterruptedException {
        MemoryStorage storage = new MemoryStorage();
        DefaultNode node = newSingleNode(storage);
        int proposeCnt = 20;
        int nThreads = 1;
        ExecutorService executors = Executors.newFixedThreadPool(nThreads);
        for (int i = 0; i < nThreads; i++) {
            int finalI = i;
            executors.execute(() -> {
                for (int j = 0; j < proposeCnt; j++) {
                    Tuple<Long, CommitContext.CommitResult> res =
                            node.propose(String.valueOf(finalI * proposeCnt + j).getBytes());
                    assertEquals(CommitContext.CommitResult.OK, res.getSecond());
                }
            });
        }
        executors.shutdown();
        while (true) {
            Ready rd = node.ready().read();
            rd.getMessages().forEach(node::step);
            storage.append(rd.getChosenValues());
            if (rd.getChosenValues().size() > 0)
                TimeUnit.MILLISECONDS.sleep(1);
            else if (rd.getMessages().size() > 0)
                TimeUnit.MILLISECONDS.sleep(1);
            node.advance();
            if (rd.getInstanceId() == proposeCnt * nThreads)
                break;
        }
        executors.shutdownNow();
    }

    @Test
    public void proposeOnDifferentNode() throws InterruptedException {
        Network network = newNetwork(3);
        network.propose(1, "123".getBytes());
        network.propose(2, "456".getBytes());
        network.propose(3, "789".getBytes());
        assertEquals(3, network.getInstance(1).nowInstanceId());
        assertEquals(3, network.getInstance(1).nowInstanceId());
        assertEquals(3, network.getInstance(1).nowInstanceId());
    }
}
