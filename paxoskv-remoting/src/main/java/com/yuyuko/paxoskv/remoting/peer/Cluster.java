package com.yuyuko.paxoskv.remoting.peer;

import com.yuyuko.paxoskv.core.PaxosMessage;
import com.yuyuko.paxoskv.remoting.peer.client.NettyPeerClient;
import com.yuyuko.paxoskv.remoting.peer.client.NettyPeerClientConfig;
import com.yuyuko.paxoskv.remoting.peer.server.NettyPeerServer;
import com.yuyuko.paxoskv.remoting.peer.server.NettyPeerServerConfig;

import java.util.List;

public class Cluster implements PeerMessageSender {
    private NettyPeerServer server;

    private NettyPeerClient client;

    public Cluster(long id,
                   NettyPeerServerConfig serverConfig,
                   NettyPeerClientConfig clientConfig,
                   PeerMessageProcessor processor,
                   List<PeerNode> peerNodes) {
        PeerChannelManager channelManager = new PeerChannelManager();
        server = new NettyPeerServer(id, serverConfig, processor, channelManager);
        client = new NettyPeerClient(id, clientConfig, peerNodes, processor, channelManager);
    }

    public void start() {
        server.start();
        client.start();
    }

    public void shutdown() {
        server.shutdown();
        client.shutdown();
    }

    @Override
    public void sendMessageToPeer(List<PaxosMessage> messages) {
        if (messages != null && messages.size() > 0) {
            client.sendMessage(messages);
        }
    }
}