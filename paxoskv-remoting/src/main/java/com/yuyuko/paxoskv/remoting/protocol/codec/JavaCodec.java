package com.yuyuko.paxoskv.remoting.protocol.codec;

public interface JavaCodec {
    <T> byte[] encode(T o);

    <T> T decode(byte[] bytes, Class<T> clazz);
}
