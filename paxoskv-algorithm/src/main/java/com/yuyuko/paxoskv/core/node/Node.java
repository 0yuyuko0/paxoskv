package com.yuyuko.paxoskv.core.node;

import com.yuyuko.paxoskv.core.CommitContext;
import com.yuyuko.paxoskv.core.PaxosMessage;
import com.yuyuko.paxoskv.core.utils.Tuple;
import com.yuyuko.selector.Channel;

public interface Node {
    /**
     * 应用层每次tick时需要调用该函数，将会由这里驱动paxos的一些操作比如learn等。
     * 至于tick的单位是多少由应用层自己决定，只要保证是恒定时间都会来调用一次就好了
     */
    void tick();

    /**
     * 提议，阻塞的
     * @param value 提议的值
     * @return 提议成功时的instanceId与提议结果
     */
    Tuple<Long, CommitContext.CommitResult> propose(byte[] value);

    /**
     * 这里是核心函数，将返回Ready的queue，应用层需要关注这个queue，当发生变更时将其中的数据进行操作
     *
     * @return Ready的阻塞队列
     */
    Channel<Ready> ready();

    /**
     * 执行消息
     *
     * @param m 执行的消息
     */
    void step(PaxosMessage m);

    /**
     * Advance函数是当使用者已经将上一次Ready数据处理之后，调用该函数告诉paxos可以进行下一步的操作
     */
    void advance();
}
