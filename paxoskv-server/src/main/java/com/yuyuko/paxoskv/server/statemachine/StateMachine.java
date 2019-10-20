package com.yuyuko.paxoskv.server.statemachine;

import com.yuyuko.paxoskv.remoting.protocol.body.ProposeMessage;
import com.yuyuko.paxoskv.remoting.protocol.codec.ProtostuffCodec;
import com.yuyuko.paxoskv.remoting.server.ClientRequest;
import com.yuyuko.paxoskv.remoting.server.ClientRequestProcessor;
import com.yuyuko.selector.Channel;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class StateMachine {
    private final Map<String, String> map = new ConcurrentHashMap<>();

    public StateMachine(Channel<byte[]> applyChan) {
        Thread thread = new Thread(() -> readApply(applyChan));
        thread.setName("StateMachineApplied");
        thread.start();
    }

    private void readApply(Channel<byte[]> applyChan) {
        while (true) {
            byte[] data = applyChan.read();
            if(data == null)
                throw new NullPointerException();
            ClientRequest request = ProtostuffCodec.getInstance().decode(data,
                    ClientRequest.class);
            ProposeMessage proposeMessage =
                    ProtostuffCodec.getInstance().decode(request.getBody()
                            , ProposeMessage.class);
            map.put(proposeMessage.getKey(), proposeMessage.getValue());
        }
    }

    public String get(String key) {
        return map.get(key);
    }
}
