package com.yuyuko.paxoskv.core.storage;

import com.yuyuko.paxoskv.core.ChosenValue;

import java.util.List;

public interface Storage {
    /**
     * @param instanceId
     * @return instanceId处的chosenValue
     */
    ChosenValue get(long instanceId);

    /**
     * @param fromInstanceId
     * @return 从instanceId处开始的chosenValue列表
     */
    List<ChosenValue> list(long fromInstanceId);


    /**
     *
     * @param fromInstanceId from
     * @param toInstanceId to 不包括
     * @return chosenValues
     */
    List<ChosenValue> list(long fromInstanceId, long toInstanceId);

    /**
     * @return 最大的instanceId
     * @throws DataNotFoundException 无数据时
     */
    long maxInstanceId();

}