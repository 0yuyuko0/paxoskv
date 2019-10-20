package com.yuyuko.paxoskv.core.storage;

import com.yuyuko.paxoskv.core.PaxosException;
import com.yuyuko.paxoskv.core.ChosenValue;
import com.yuyuko.paxoskv.core.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Unstable {
    private static final Logger log = LoggerFactory.getLogger(Unstable.class);

    private List<ChosenValue> chosenValues = new ArrayList<>();

    /**
     * chosenValues第一条数据的索引，如果没有数据则是-1
     */
    private long minInstanceId;

    public Unstable(Storage storage) {
        try {
            minInstanceId = storage.maxInstanceId() + 1;
        } catch (DataNotFoundException ex) {
            minInstanceId = -1;
        }
    }

    public void append(ChosenValue chosenValue) {
        long instanceId = chosenValue.getInstanceId();
        if (isEmpty()) {
            chosenValues.add(chosenValue);
            minInstanceId = instanceId;
        } else {
            //修改
            if (instanceId == minInstanceId + chosenValues.size() - 1)
                chosenValues.set(chosenValues.size() - 1, chosenValue);
                //新增
            else if (instanceId == minInstanceId + chosenValues.size())
                chosenValues.add(chosenValue);
            else {//大于，不太可能
                log.error("[Append Fail] chosenValue with instanceId {} > maxInstanceId + 1 {}",
                        instanceId, minInstanceId + chosenValues.size());
                throw new PaxosException();
            }
        }
    }

    public void append(List<ChosenValue> values) {
        if (Utils.isEmpty(values))
            return;
        long firstInstanceId = values.get(0).getInstanceId();
        long lastInstanceId = values.get(values.size() - 1).getInstanceId();
        if (isEmpty()) {
            minInstanceId = firstInstanceId;
            chosenValues.addAll(values);
            return;
        }
        if (lastInstanceId <= maxInstanceId())//最后一个数据比最大的还小，不需要
            return;
        //传入数据的第一个比minInstanceId还小，截断到minInstanceId
        if (firstInstanceId < minInstanceId) {
            int overlap = ((int) (minInstanceId - firstInstanceId));
            if (overlap < values.size())
                values = values.subList(overlap, values.size());
            else//截断到空了，返回
                return;
            firstInstanceId = minInstanceId;
        }
        if (firstInstanceId == maxInstanceId() + 1) {
            // 如果正好是紧接着当前数据的，就直接append
            chosenValues.addAll(values);
        } else if (firstInstanceId < maxInstanceId() + 1) {//小于，需要截断
            values = values.subList(((int) (maxInstanceId() + 1 - firstInstanceId)),
                    ((int) (lastInstanceId + 1 - firstInstanceId)));
            chosenValues.addAll(values);
        } else {//大于，不太可能
            log.error("[Unstable Append Error] maxInstanceId[{}], and appendValues " +
                            "minInstanceId[{}]",
                    maxInstanceId(), minInstanceId);
            throw new PaxosException();
        }
    }

    public ChosenValue getChosenValue(long instanceId) {
        if (instanceId < minInstanceId || isEmpty())
            return null;
        if (instanceId >= minInstanceId + chosenValues.size())
            throw new DataNotFoundException();
        return chosenValues.get(((int) (instanceId - minInstanceId)));
    }

    public List<ChosenValue> listAcceptedValuesFrom(long fromInstanceId) {
        if (isEmpty())
            return new ArrayList<>();
        if (fromInstanceId >= minInstanceId + chosenValues.size())
            throw new DataNotFoundException();
        //小于，只能返回当前的了
        if (fromInstanceId < minInstanceId)
            return findAcceptedValues(new ArrayList<>(chosenValues));

        return findAcceptedValues(slice(fromInstanceId,
                minInstanceId + chosenValues.size()));
    }

    public List<ChosenValue> slice(long lo, long hi) {
        checkOutOfBounds(lo, hi);
        return new ArrayList<>(chosenValues.subList(((int) (lo - minInstanceId)),
                ((int) (hi - minInstanceId))));
    }

    private void checkOutOfBounds(long lo, long hi) {
        if (lo > hi) {
            log.error("[Invalid unstable.slice] {} > {}", lo, hi);
            throw new PaxosException();
        }

        long upper = minInstanceId + chosenValues.size();
        if (lo < minInstanceId || hi > upper) {
            log.error("[Slice out of Bound],unstable.slice[{},{}) out of bound [{},{}]", lo, hi,
                    minInstanceId,
                    upper);
            throw new PaxosException();
        }
    }

    public void stableTo(long instanceId) {
        chosenValues =
                new ArrayList<>(chosenValues.subList(((int) (instanceId - minInstanceId + 1)),
                        chosenValues.size()));
        minInstanceId = isEmpty() ? -1 : chosenValues.get(0).getInstanceId();
        log.debug("[Unstable Stable To] stableTo {}", instanceId);
    }

    public ChosenValue lastChosenValue() {
        return isEmpty() ? null : chosenValues.get(chosenValues.size() - 1);
    }

    public List<ChosenValue> acceptedValues() {
        return findAcceptedValues(chosenValues);
    }

    private List<ChosenValue> findAcceptedValues(List<ChosenValue> chosenValues) {
        int pos = chosenValues.size();
        for (int i = 0; i < chosenValues.size(); i++) {
            if (chosenValues.get(i).getAcceptedValue() == null) {
                pos = i;
                break;
            }
        }
        if (pos == chosenValues.size())
            return chosenValues;
        return new ArrayList<>(chosenValues.subList(0, pos));
    }

    public long minInstanceId() {
        if (isEmpty()) throw new DataNotFoundException();
        return minInstanceId;
    }

    public long maxInstanceId() {
        if (isEmpty()) throw new DataNotFoundException();
        return minInstanceId + chosenValues.size() - 1;
    }

    public boolean isEmpty() {
        return chosenValues.size() == 0;
    }
}
