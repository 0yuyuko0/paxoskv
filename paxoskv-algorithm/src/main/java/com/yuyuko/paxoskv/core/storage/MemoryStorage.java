package com.yuyuko.paxoskv.core.storage;

import com.yuyuko.paxoskv.core.ChosenValue;
import com.yuyuko.paxoskv.core.PaxosException;
import com.yuyuko.paxoskv.core.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MemoryStorage implements Storage {
    private static final Logger log = LoggerFactory.getLogger(MemoryStorage.class);

    private List<ChosenValue> chosenValues = new ArrayList<>();

    /**
     * chosenValues.get(0).getInstanceId()
     */
    private long minInstanceId;

    /**
     * instanceId是否发生了变化
     */
    private boolean newInstance;

    /**
     * 当前paxosInstance的instanceId
     */
    private long instanceId;

    private boolean isEmpty() {
        return chosenValues.isEmpty();
    }

    public void saveInstanceId(long instanceId) {
        if (instanceId > this.instanceId) {
            this.instanceId = instanceId;
            newInstance = true;
        }
    }

    public boolean isNewInstance() {
        try {
            return newInstance;
        } finally {
            newInstance = false;
        }
    }

    public void append(List<ChosenValue> appendValues) {
        if (Utils.isEmpty(appendValues))
            return;
        long firstInstanceId = appendValues.get(0).getInstanceId();
        long lastInstanceId = appendValues.get(appendValues.size() - 1).getInstanceId();
        if (isEmpty()) {
            minInstanceId = firstInstanceId;
            chosenValues.addAll(appendValues);
            return;
        }
        if (lastInstanceId <= maxInstanceId())//最后一个数据比最大的还小，不需要
            return;
        //传入数据的第一个比minInstanceId还小，截断到minInstanceId
        if (firstInstanceId < minInstanceId) {
            int overlap = ((int) (minInstanceId - firstInstanceId));
            if (overlap < appendValues.size())
                appendValues = appendValues.subList(overlap, appendValues.size());
            else//截断到空了，返回
                return;
            firstInstanceId = minInstanceId;
        }
        if (firstInstanceId == maxInstanceId() + 1) {
            // 如果正好是紧接着当前数据的，就直接append
            chosenValues.addAll(appendValues);
        } else if (firstInstanceId < maxInstanceId() + 1) {//小于，需要截断
            appendValues = appendValues.subList(((int) (maxInstanceId() + 1 - firstInstanceId)),
                    ((int) (lastInstanceId + 1 - firstInstanceId)));
            chosenValues.addAll(appendValues);
        } else {//大于，不太可能
            log.error("[Storage Append Error] maxInstanceId[{}], and appendValues " +
                            "firstInstanceId[{}]",
                    maxInstanceId(), firstInstanceId);
            throw new PaxosException();
        }
    }

    @Override
    public ChosenValue get(long instanceId) {
        if (isEmpty() || instanceId < minInstanceId || instanceId > maxInstanceId())
            throw new DataNotFoundException();
        return chosenValues.get(((int) (instanceId - minInstanceId)));
    }

    @Override
    public List<ChosenValue> list(long fromInstanceId) {
        if (illegalBound(fromInstanceId, maxInstanceId() + 1))
            throw new DataNotFoundException();
        return slice(fromInstanceId, maxInstanceId() + 1);
    }

    /**
     * @param fromInstanceId 包括
     * @param toInstanceId   不包括
     * @return slice
     */
    private List<ChosenValue> slice(long fromInstanceId, long toInstanceId) {
        return new ArrayList<>(chosenValues.subList(((int) (fromInstanceId - minInstanceId)),
                ((int) (toInstanceId - minInstanceId))));
    }

    @Override
    public List<ChosenValue> list(long fromInstanceId, long toInstanceId) {
        if (illegalBound(fromInstanceId, toInstanceId)) {
            log.error("fromInstanceId {}, minInstanceId {},toInstanceId {}, maxInstanceId {}",
                    fromInstanceId, minInstanceId, toInstanceId, maxInstanceId());
            throw new DataNotFoundException();
        }
        return slice(fromInstanceId, toInstanceId);
    }

    private boolean illegalBound(long lo, long hi) {
        if (isEmpty()) return true;
        return lo < minInstanceId || lo > maxInstanceId()
                || hi > maxInstanceId() + 1 || lo > hi;
    }

    @Override
    public long maxInstanceId() {
        if (isEmpty())
            throw new DataNotFoundException();
        return minInstanceId + chosenValues.size() - 1;
    }
}
