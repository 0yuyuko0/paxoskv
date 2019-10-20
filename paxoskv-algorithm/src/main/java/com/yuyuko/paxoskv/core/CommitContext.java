package com.yuyuko.paxoskv.core;

import com.yuyuko.paxoskv.core.PaxosException;
import com.yuyuko.paxoskv.core.utils.Tuple;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class CommitContext {
    private volatile long instanceId;

    private volatile byte[] value;

    private volatile boolean isCommitEnd;

    private volatile CommitResult result;

    private final ReentrantLock lock = new ReentrantLock();

    private final Condition end = lock.newCondition();

    public CommitContext() {
        newCommit(null);
    }

    public enum CommitResult {
        OK,
        Conflict;
    }

    public void newCommit(byte[] value) {
        lock.lock();
        instanceId = -1;
        result = null;
        isCommitEnd = false;
        this.value = value;
        lock.unlock();
    }

    public void setResult(CommitResult result, long instanceId,
                          byte[] learnValue) {
        try {
            lock.lock();
            if (isCommitEnd || this.instanceId != instanceId)
                return;
            this.result = result;
            if (result == CommitResult.OK)
                if (!Arrays.equals(this.value, learnValue))
                    this.result = CommitResult.Conflict;
            this.isCommitEnd = true;
            this.value = null;
        } finally {
            end.signal();
            lock.unlock();
        }
    }

    public Tuple<Long, CommitResult> getResult() {
        try {
            lock.lock();
            while (!isCommitEnd) {
                end.await();
            }
            if (result == CommitResult.OK)
                return new Tuple<>(instanceId, CommitResult.OK);
            return new Tuple<>(0L, result);
        } catch (InterruptedException e) {
            throw new PaxosException(e);
        } finally {
            lock.unlock();
        }
    }

    public boolean isNewCommit() {
        return instanceId == -1 && value != null;
    }

    public void startCommit(long instanceId) {
        lock.lock();
        this.instanceId = instanceId;
        lock.unlock();
    }
}

