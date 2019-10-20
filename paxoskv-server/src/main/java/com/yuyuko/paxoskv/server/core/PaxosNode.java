package com.yuyuko.paxoskv.server.core;

import com.yuyuko.paxoskv.core.ChosenValue;
import com.yuyuko.paxoskv.core.CommitContext;
import com.yuyuko.paxoskv.core.PaxosException;
import com.yuyuko.paxoskv.core.PaxosMessage;
import com.yuyuko.paxoskv.core.node.*;
import com.yuyuko.paxoskv.core.storage.MemoryStorage;
import com.yuyuko.paxoskv.core.utils.Tuple;
import com.yuyuko.paxoskv.core.utils.Utils;
import com.yuyuko.paxoskv.remoting.peer.PeerMessageProcessor;
import com.yuyuko.paxoskv.remoting.protocol.RequestCode;
import com.yuyuko.paxoskv.remoting.protocol.ResponseCode;
import com.yuyuko.paxoskv.remoting.protocol.body.ReadMessage;
import com.yuyuko.paxoskv.remoting.protocol.codec.ProtostuffCodec;
import com.yuyuko.paxoskv.remoting.server.ClientRequest;
import com.yuyuko.paxoskv.remoting.server.ClientRequestProcessor;
import com.yuyuko.paxoskv.remoting.server.ClientResponse;
import com.yuyuko.paxoskv.server.statemachine.StateMachine;
import com.yuyuko.selector.Channel;
import com.yuyuko.selector.SelectionKey;
import com.yuyuko.selector.Selector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;

public class PaxosNode implements PeerMessageProcessor, ClientRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(PaxosNode.class);

    private long id;

    private List<Long> peers;

    /**
     * newInstance时写入，让StateMachine消费
     */
    private Channel<byte[]> applyChan;

    private long applyInstanceId = -1;

    private Node node;

    private MemoryStorage storage = new MemoryStorage();

    private Timer timer = new Timer("TickTask", true);

    private StateMachine stateMachine;

    public static Tuple<PeerMessageProcessor, ClientRequestProcessor>
    newPaxosNode(long id,
                 List<Long> peers) {
        PaxosNode paxosNode = new PaxosNode();
        paxosNode.id = id;
        paxosNode.peers = peers;
        paxosNode.startPaxos();
        paxosNode.applyChan = new Channel<>();
        paxosNode.stateMachine = new StateMachine(paxosNode.applyChan);
        return new Tuple<>(paxosNode, paxosNode);
    }

    private void startPaxos() {
        List<NodeInfo> nodeInfos = peers.stream().map(NodeInfo::new).collect(Collectors.toList());
        Config config = new Config();
        config.setStorage(storage);
        config.setNodeInfoList(nodeInfos);
        config.setNodeCount(nodeInfos.size());
        config.setNodeId(id);
        node = DefaultNode.startNode(config);

        Thread thread = new Thread(this::serveChannels);
        thread.setName("ServerChannelEventLoop");

        thread.start();
    }

    private void serveChannels() {
        final Channel<Object> tickChan = new Channel<>();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                tickChan.write(null);
            }
        }, 0, 1);

        while (true) {
            SelectionKey<?> key = Selector.open()
                    .register(tickChan, SelectionKey.read())
                    .register(node.ready(), SelectionKey.read())
                    .select();
            if (key.channel() == tickChan)
                node.tick();
            else if (key.channel() == node.ready()) {
                Ready rd = key.data(Ready.class);
                storage.append(rd.getChosenValues());
                storage.saveInstanceId(rd.getInstanceId());
                if (storage.isNewInstance()) {
                    publishChosenValuesToStateMachine();
                }
                if (Utils.notEmpty(rd.getMessages())) {
                    List<PaxosMessage> remoteMessages = new ArrayList<>();
                    rd.getMessages().forEach(m -> {
                        if (m.getToNodeId() == id)
                            node.step(m);
                        else
                            remoteMessages.add(m);
                    });

                    Server.sendMessageToPeer(remoteMessages);
                }
                node.advance();
            } else
                throw new RuntimeException();
        }
    }

    private void publishChosenValuesToStateMachine() {
        long maxInstanceId = storage.maxInstanceId();
        if (applyInstanceId == maxInstanceId)
            return;
        List<ChosenValue> valuesToApply = storage.list(applyInstanceId + 1, maxInstanceId + 1);
        for (ChosenValue chosenValue : valuesToApply) {
            applyChan.write(chosenValue.getAcceptedValue());
            applyInstanceId = chosenValue.getInstanceId();
        }
    }

    @Override
    public void process(PaxosMessage message) {
        node.step(message);
    }

    @Override
    public void processRequest(ClientRequest clientRequest) {
        if (clientRequest.getCode() == RequestCode.PROPOSE)
            propose(clientRequest);
        else
            read(clientRequest);
    }

    public void propose(ClientRequest request) {
        Tuple<Long, CommitContext.CommitResult> res =
                node.propose(ProtostuffCodec.getInstance().encode(request));
        CommitContext.CommitResult result = res.getSecond();
        switch (result) {
            case OK:
                Server.sendResponseToClient(request.getRequestId(),
                        new ClientResponse(ResponseCode.PROPOSE, "Propose Success".getBytes()));
                break;
            case Conflict:
                Server.sendResponseToClient(request.getRequestId(),
                        new ClientResponse(ResponseCode.PROPOSE, "Propose Failed".getBytes()));
                break;
        }
    }

    public void read(ClientRequest request) {
        ReadMessage readMessage = ProtostuffCodec.getInstance().decode(request.getBody(),
                ReadMessage.class);
        String value = stateMachine.get(readMessage.getKey());
        if (value == null)
            value = "(null)";
        Server.sendResponseToClient(request.getRequestId(),
                new ClientResponse(ResponseCode.READ, value.getBytes()));
    }
}