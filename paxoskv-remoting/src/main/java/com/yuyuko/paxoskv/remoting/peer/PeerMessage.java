package com.yuyuko.paxoskv.remoting.peer;

import com.yuyuko.paxoskv.core.PaxosMessage;

/**
 * 节点之间通信的消息
 * 为null表示心跳消息（不是raft里的心跳）
 */
public class PeerMessage {
    private PaxosMessage message;

    private PeerMessageType type;

    public enum PeerMessageType {
        Normal,
        Heartbeat,
        DoNotReconnect
    }

    public PeerMessage() {
    }

    public PeerMessage(PaxosMessage message) {
        this(message, PeerMessageType.Normal);
    }

    private PeerMessage(PaxosMessage message, PeerMessageType type) {
        this.message = message;
        this.type = type;
    }

    public static PeerMessage heartbeat(long id) {
        return new PeerMessage(PaxosMessage.builder().nodeId(id).build(), PeerMessageType.Heartbeat);
    }

    public static PeerMessage doNotReconnect(long id) {
        return new PeerMessage(PaxosMessage.builder().nodeId(id).build(),
                PeerMessageType.DoNotReconnect);
    }

    public PaxosMessage getMessage() {
        return message;
    }

    public PeerMessageType getType() {
        return type;
    }
}
