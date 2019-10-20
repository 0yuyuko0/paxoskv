package com.yuyuko.paxoskv.server.core;

import com.yuyuko.paxoskv.core.PaxosMessage;
import com.yuyuko.paxoskv.remoting.peer.Cluster;
import com.yuyuko.paxoskv.remoting.peer.PeerMessageProcessor;
import com.yuyuko.paxoskv.remoting.peer.PeerNode;
import com.yuyuko.paxoskv.remoting.peer.client.NettyPeerClientConfig;
import com.yuyuko.paxoskv.remoting.peer.server.NettyPeerServerConfig;
import com.yuyuko.paxoskv.remoting.server.*;

import java.util.List;

public class Server {
    private final NettyServer server;

    private final Cluster cluster;

    private static volatile Server globalInstance;

    public Server(long id,
                  int port,
                  ClientRequestProcessor requestProcessor,
                  List<PeerNode> peerNodes,
                  PeerMessageProcessor messageProcessor) {
        NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setListenPort(port);

        server = new NettyServer(id, serverConfig, requestProcessor);


        NettyPeerServerConfig peerServerConfig = new NettyPeerServerConfig();
        peerServerConfig.setListenPort(port + NettyPeerServerConfig.PEER_PORT_INCREMENT);

        cluster = new Cluster(id, peerServerConfig,
                new NettyPeerClientConfig(), messageProcessor, peerNodes);
        globalInstance = this;
    }

    public void start() {
        cluster.start();
        server.start();
    }

    public static void sendMessageToPeer(List<PaxosMessage> messages) {
        if (globalInstance != null)
            globalInstance.cluster.sendMessageToPeer(messages);
    }

    public static void sendResponseToClient(String requestId, ClientResponse response) {
        if (globalInstance != null)
            globalInstance.server.sendResponseToClient(requestId, response);
    }
}