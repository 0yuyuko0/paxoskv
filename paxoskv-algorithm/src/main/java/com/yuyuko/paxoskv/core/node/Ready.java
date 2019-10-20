package com.yuyuko.paxoskv.core.node;

import com.yuyuko.paxoskv.core.ChosenValue;
import com.yuyuko.paxoskv.core.PaxosException;
import com.yuyuko.paxoskv.core.PaxosInstance;
import com.yuyuko.paxoskv.core.PaxosMessage;
import com.yuyuko.paxoskv.core.utils.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Ready {
    private long instanceId = -1;
    /**
     * 需要持久化的value
     */
    private final List<ChosenValue> chosenValues;

    /**
     * 需要发送的message
     */
    private final List<PaxosMessage> messages;

    public Ready(PaxosInstance paxosInstance, long prevInstanceId) {
        chosenValues =
                List.copyOf(paxosInstance.getPaxosLog().getUnstable().acceptedValues());
        messages = List.copyOf(paxosInstance.getMessages());

        if (paxosInstance.nowInstanceId() != prevInstanceId)
            this.instanceId = paxosInstance.nowInstanceId();
    }

    public boolean containsUpdate() {
        return instanceId != -1 || Utils.notEmpty(chosenValues) || Utils.notEmpty(messages);
    }

    public List<ChosenValue> getChosenValues() {
        return chosenValues;
    }

    public List<PaxosMessage> getMessages() {
        return messages;
    }

    public long getInstanceId() {
        return instanceId;
    }
}
