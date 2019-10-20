package com.yuyuko.paxoskv.core;

import com.yuyuko.paxoskv.core.utils.Tuple;
import com.yuyuko.selector.Channel;

import java.util.function.Consumer;

public class Committer {
    private final CommitContext commitContext;

    private volatile Consumer<byte[]> proposeCallback;

    public Committer(CommitContext commitContext) {
        this.commitContext = commitContext;
    }

    /**
     * 提议一个值
     *
     * @return instanceId,commitResult
     */
    public Tuple<Long, CommitContext.CommitResult> propose(byte[] value) {
        int retryCount = 3;
        Tuple<Long, CommitContext.CommitResult> tuple = null;
        while (retryCount-- != 0) {
            tuple = proposeNotRetry(value);
            if (tuple.getSecond() != CommitContext.CommitResult.Conflict) {
                return tuple;
            }
        }
        return tuple;
    }

    private synchronized Tuple<Long, CommitContext.CommitResult> proposeNotRetry(byte[] value) {
        commitContext.newCommit(value);
        //通知有提交了
        proposeCallback.accept(value);
        return commitContext.getResult();
    }

    public void setProposeCallback(Consumer<byte[]>proposeCallback){
        this.proposeCallback = proposeCallback;
    }
}
