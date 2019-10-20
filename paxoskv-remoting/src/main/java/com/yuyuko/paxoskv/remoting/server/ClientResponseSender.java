package com.yuyuko.paxoskv.remoting.server;

public interface ClientResponseSender {
    void sendResponseToClient(String requestId, ClientResponse response);
}
