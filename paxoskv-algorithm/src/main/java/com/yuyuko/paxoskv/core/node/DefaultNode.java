package com.yuyuko.paxoskv.core.node;

import com.yuyuko.paxoskv.core.CommitContext;
import com.yuyuko.paxoskv.core.PaxosException;
import com.yuyuko.paxoskv.core.PaxosInstance;
import com.yuyuko.paxoskv.core.PaxosMessage;
import com.yuyuko.paxoskv.core.utils.Tuple;
import com.yuyuko.paxoskv.core.utils.Utils;
import com.yuyuko.selector.Channel;
import com.yuyuko.selector.SelectionKey;
import com.yuyuko.selector.Selector;

import static com.yuyuko.selector.SelectionKey.read;
import static com.yuyuko.selector.SelectionKey.write;

public class DefaultNode implements Node {

    private final Channel<PaxosMessage> recvChan;

    private final Channel<Object> tickChan;

    private final Channel<byte[]> propChan;

    private final Channel<Ready> readyChan;

    private final Channel<Object> advanceChan;

    private final PaxosInstance paxosInstance;

    private DefaultNode(PaxosInstance paxosInstance) {
        recvChan = new Channel<>();
        tickChan = new Channel<>(128);
        propChan = new Channel<>();
        readyChan = new Channel<>();
        advanceChan = new Channel<>();
        this.paxosInstance = paxosInstance;
    }

    public static DefaultNode startNode(Config config) {
        PaxosInstance paxosInstance = new PaxosInstance(config);
        DefaultNode node = new DefaultNode(paxosInstance);
        paxosInstance.getCommitter().setProposeCallback(node::committerProposeCallback);

        Thread thread = new Thread(node::run);
        thread.setName("PaxosNodeEventLoop");
        thread.start();
        return node;
    }

    private void committerProposeCallback(byte[] data) {
        propChan.write(data);
    }


    private void run() {
        Channel<Ready> readyChan;
        Channel<Object> advanceChan = null;
        Long prevUnstableMinInstanceId = null;
        long prevInstanceId = -1;
        Ready rd = null;
        while (true) {
            if (advanceChan != null)
                // advance channel不为空，说明还在等应用调用Advance接口通知已经处理完毕了本次的ready数据
                readyChan = null;
            else {
                rd = new Ready(paxosInstance, prevInstanceId);
                if (rd.containsUpdate())
                    readyChan = this.readyChan;
                else
                    readyChan = null;
            }
            SelectionKey<?> key =
                    Selector.open()
                            .register(tickChan, read())
                            .register(propChan, read())
                            .register(recvChan, read())
                            .register(readyChan, write(rd))
                            .register(advanceChan, read())
                            .select();
            if (key.channel() == propChan) {
                paxosInstance.propose(((byte[]) key.data()));
            } else if (key.channel() == recvChan) {
                paxosInstance.step(((PaxosMessage) key.data()));
            } else if (key.channel() == tickChan) {
                paxosInstance.tick();
            } else if (key.channel() == readyChan) {
                if (Utils.notEmpty(rd.getChosenValues())) {
                    prevUnstableMinInstanceId =
                            rd.getChosenValues().get(rd.getChosenValues().size() - 1).getInstanceId();
                }
                prevInstanceId = paxosInstance.nowInstanceId();
                paxosInstance.clearMessages();
                advanceChan = this.advanceChan;
            } else if (key.channel() == advanceChan) {
                if (prevUnstableMinInstanceId != null) {
                    paxosInstance.getPaxosLog().stableTo(prevUnstableMinInstanceId);
                    prevUnstableMinInstanceId = null;
                }
                advanceChan = null;
            }
        }
    }

    @Override
    public void tick() {
        tickChan.write(null);
    }

    @Override
    public Tuple<Long, CommitContext.CommitResult> propose(byte[] value) {
        return paxosInstance.getCommitter().propose(value);
    }

    @Override
    public Channel<Ready> ready() {
        return readyChan;
    }

    @Override
    public void step(PaxosMessage m) {
        recvChan.write(m);
    }

    public PaxosInstance getPaxosInstance() {
        return paxosInstance;
    }

    @Override
    public void advance() {
        advanceChan.write(null);
    }
}
