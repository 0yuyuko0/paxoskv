package com.yuyuko.paxoskv.remoting.peer;

import com.yuyuko.paxoskv.remoting.protocol.codec.ProtostuffCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.ByteBuffer;

public class PeerMessageEncoder extends MessageToByteEncoder<PeerMessage> {
    @Override
    protected void encode(ChannelHandlerContext ctx, PeerMessage msg, ByteBuf out) throws Exception {
        byte[] bytes = ProtostuffCodec.getInstance().encode(msg);
        int length = 4;
        ByteBuffer res = ByteBuffer.allocate(length + bytes.length);
        res.putInt(bytes.length);
        res.put(bytes);
        res.flip();
        out.writeBytes(res);
    }
}
