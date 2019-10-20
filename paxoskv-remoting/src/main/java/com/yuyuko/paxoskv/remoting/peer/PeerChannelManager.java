package com.yuyuko.paxoskv.remoting.peer;

import com.yuyuko.paxoskv.remoting.ChannelManager;
import io.netty.channel.ChannelHandlerContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PeerChannelManager implements ChannelManager<Long> {
    private final Map<Long, ChannelHandlerContext> map = new ConcurrentHashMap<>();

    @Override
    public boolean registerChannel(Long id, ChannelHandlerContext ctx) {
        synchronized (map) {
            ChannelHandlerContext old = map.get(id);
            if (old == null) {
                map.put(id, ctx);
                return true;
            }
            return false;
        }
    }

    @Override
    public ChannelHandlerContext getChannel(Long id) {
        return map.get(id);
    }

    @Override
    public boolean removeChannel(Long id) {
        return map.remove(id) != null;
    }
}
