package com.yuyuko.paxoskv.core.storage;

import com.yuyuko.paxoskv.core.ChosenValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PaxosLog {
    private static final Logger log = LoggerFactory.getLogger(PaxosLog.class);

    private Storage storage;

    private Unstable unstable;

    public PaxosLog(Storage storage) {
        this.storage = storage;
        unstable = new Unstable(storage);
    }

    /**
     * 读取指定groupId与instanceId的提议
     *
     * @param instanceId instanceId
     * @return ChosenValue
     * @throws DataNotFoundException instanceId找不到时
     */
    public ChosenValue readChosenValue(long instanceId) {
        ChosenValue chosenValue = unstable.getChosenValue(instanceId);
        if (chosenValue != null)
            return chosenValue;

        return storage.get(instanceId);
    }

    public List<ChosenValue> listAcceptedValuesFrom(long instanceId) {
        if (unstable.isEmpty())
            return storage.list(instanceId);
        if (instanceId >= unstable.minInstanceId()) {
            return unstable.listAcceptedValuesFrom(instanceId);
        } else {
            //需要拼接
            List<ChosenValue> last = unstable.listAcceptedValuesFrom(instanceId);
            List<ChosenValue> first = storage.list(instanceId, unstable.minInstanceId());
            first.addAll(last);
            return first;
        }
    }

    /**
     * @param learnedValues append的值
     * @return 最新的ChosenValue
     */
    public ChosenValue maybeAppend(List<ChosenValue> learnedValues, long nowInstanceId) {
        long lastInstanceId = learnedValues.get(learnedValues.size() - 1).getInstanceId();
        if (lastInstanceId >= nowInstanceId) {
            unstable.append(learnedValues);
        }
        return learnedValues.get(learnedValues.size() - 1);
    }

    /**
     * 获取Storage中最大的instanceId
     *
     * @return instanceId
     * @throws DataNotFoundException DataNotFoundException
     */
    public long maxInstanceId() {
        try {
            return storage.maxInstanceId();
        } catch (DataNotFoundException ex) {
            log.info("MaxInstanceId not exists");
            throw ex;
        }
    }

    public Unstable getUnstable() {
        return unstable;
    }

    public void stableTo(long instanceId) {
        unstable.stableTo(instanceId);
    }
}
