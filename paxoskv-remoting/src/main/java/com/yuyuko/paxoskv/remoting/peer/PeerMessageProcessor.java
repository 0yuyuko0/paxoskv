package com.yuyuko.paxoskv.remoting.peer;

import com.yuyuko.paxoskv.core.PaxosMessage;

public interface PeerMessageProcessor {
    void process(PaxosMessage message);
}
