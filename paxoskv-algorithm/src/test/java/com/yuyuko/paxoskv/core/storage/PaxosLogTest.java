package com.yuyuko.paxoskv.core.storage;

import com.yuyuko.paxoskv.core.ChosenValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.of;

class PaxosLogTest {

    @ParameterizedTest
    @MethodSource("readChosenValueGen")
    void readChosenValue(long instanceId, boolean wEx) {
        MemoryStorage storage = new MemoryStorage();
        storage.append(List.of(new ChosenValue(0), new ChosenValue(1)));
        PaxosLog log = new PaxosLog(storage);
        log.getUnstable().append(List.of(new ChosenValue(2), new ChosenValue(3)));

        boolean hasEx = false;

        try {
            log.readChosenValue(instanceId);
        } catch (DataNotFoundException ex) {
            hasEx = true;
        }
        if (hasEx ^ wEx)
            fail();
    }

    static Stream<Arguments> readChosenValueGen() {
        return Stream.of(
                of(-1, true), of(0, false), of(2, false), of(4, true)
        );
    }

    @ParameterizedTest
    @MethodSource("readChosenValueFromGen")
    void readChosenValueFrom(long from, boolean wEx, int wSize) {
        MemoryStorage storage = new MemoryStorage();
        storage.append(List.of(new ChosenValue(0), new ChosenValue(1)));
        PaxosLog log = new PaxosLog(storage);
        log.getUnstable().append(List.of(new ChosenValue(2), new ChosenValue(3)));

        boolean hasEx = false;

        try {
            List<ChosenValue> chosenValues = log.listAcceptedValuesFrom(from);
            assertEquals(wSize, chosenValues.size());
        } catch (DataNotFoundException ex) {
            hasEx = true;
        }
        if (hasEx ^ wEx)
            fail();
    }

    static Stream<Arguments> readChosenValueFromGen() {
        return Stream.of(
                of(0, false, 4), of(1, false, 3), of(2, false, 2), of(3, false, 1),
                of(4, true, 0)
        );
    }

    @Test
    void append() {
        PaxosLog log = new PaxosLog(new MemoryStorage());
        log.maybeAppend(List.of(new ChosenValue(0), new ChosenValue(1)), 0);
        assertThrows(DataNotFoundException.class, log::maxInstanceId);
    }


    @Test
    void maxInstanceId() {
        assertThrows(DataNotFoundException.class, new PaxosLog(new MemoryStorage())
                ::maxInstanceId);

    }

}