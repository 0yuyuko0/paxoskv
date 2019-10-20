package com.yuyuko.paxoskv.remoting.peer;

import com.yuyuko.paxoskv.core.PaxosMessage;

import java.util.List;

public interface PeerMessageSender {
    void sendMessageToPeer(List<PaxosMessage> message);
}
