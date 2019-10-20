package com.yuyuko.paxoskv.remoting.protocol.body;

public class ReadMessage {
    private String key;

    public ReadMessage(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
