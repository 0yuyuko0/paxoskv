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

class UnstableTest {

    @Test
    void append() {
        Unstable unstable = new Unstable(new MemoryStorage());
        assertThrows(Throwable.class, () -> unstable.minInstanceId());
        unstable.append(new ChosenValue(0));
        assertEquals(0, unstable.minInstanceId());
        assertEquals(0, unstable.maxInstanceId());
        unstable.append(new ChosenValue(0));
        assertEquals(0, unstable.minInstanceId());
        assertEquals(0, unstable.maxInstanceId());
        unstable.append(new ChosenValue(1));
        assertEquals(1, unstable.maxInstanceId());
    }

    @Test
    void appendList() {
        Unstable unstable = new Unstable(new MemoryStorage());
        unstable.append(List.of());
        unstable.append(List.of(new ChosenValue(0)));
        assertEquals(0, unstable.maxInstanceId());
        unstable.append(List.of(new ChosenValue(0)));
        assertEquals(0, unstable.maxInstanceId());
        unstable.append(List.of(new ChosenValue(0), new ChosenValue(1)));
        assertEquals(1, unstable.maxInstanceId());
        unstable.append(List.of(new ChosenValue(0), new ChosenValue(1), new ChosenValue(2)));
        assertEquals(2, unstable.maxInstanceId());
        unstable.append(List.of(new ChosenValue(3)));
        assertEquals(3, unstable.maxInstanceId());
        unstable.append(List.of(new ChosenValue(2)));
        assertEquals(3, unstable.maxInstanceId());
        unstable.append(List.of(new ChosenValue(2), new ChosenValue(3), new ChosenValue(4)));
        assertEquals(4, unstable.maxInstanceId());
    }

    @Test
    void appendList2() {
        Unstable unstable = new Unstable(new MemoryStorage());
        unstable.append(List.of(new ChosenValue(1)));
        assertEquals(1, unstable.maxInstanceId());
        assertEquals(1, unstable.minInstanceId());
        unstable.append(List.of(new ChosenValue(0), new ChosenValue(1), new ChosenValue(2)));
        assertEquals(2, unstable.maxInstanceId());
        assertEquals(1, unstable.minInstanceId());
        unstable.append(List.of(new ChosenValue(1), new ChosenValue(2), new ChosenValue(3)));
        assertEquals(3, unstable.maxInstanceId());
        assertEquals(3, unstable.maxInstanceId());
        unstable.append(List.of(new ChosenValue(2), new ChosenValue(3), new ChosenValue(4)));
        assertEquals(4, unstable.maxInstanceId());
        assertEquals(1, unstable.minInstanceId());
    }

    @ParameterizedTest
    @MethodSource("getChosenValueGen")
    void getChosenValue(long instanceId, boolean wEx) {
        Unstable unstable = new Unstable(new MemoryStorage());
        unstable.append(List.of(new ChosenValue(2), new ChosenValue(3),
                new ChosenValue(4)));
        boolean hasEx = false;
        try {
            unstable.getChosenValue(instanceId);
        } catch (DataNotFoundException ex) {
            hasEx = true;
        }
        if (hasEx ^ wEx)
            fail();
    }

    static Stream<Arguments> getChosenValueGen() {
        return Stream.of(of(2, false),
                of(3, false), of(4, false), of(5, true));
    }


    @ParameterizedTest
    @MethodSource("listChosenValuesFromGen")
    void listChosenValuesFrom(long from, boolean wEx, int wSize) {
        Unstable unstable = new Unstable(new MemoryStorage());
        unstable.append(List.of(new ChosenValue(2), new ChosenValue(3),
                new ChosenValue(4)));
        boolean hasEx = false;

        try {
            List<ChosenValue> chosenValues = unstable.listAcceptedValuesFrom(from);
            assertEquals(wSize, chosenValues.size());
        } catch (DataNotFoundException ex) {
            hasEx = true;
        }
        if (hasEx ^ wEx)
            fail();
    }

    static Stream<Arguments> listChosenValuesFromGen() {
        return Stream.of(
                of(1, false, 3), of(2, false, 3), of(3, false, 2), of(4, false, 1),
                of(5, true, 0)
        );
    }

    @Test
    void stableTo() {
        Unstable unstable = new Unstable(new MemoryStorage());
        unstable.append(List.of(new ChosenValue(2), new ChosenValue(3),
                new ChosenValue(4)));
        unstable.stableTo(2);
        assertEquals(3, unstable.minInstanceId());
        assertEquals(4, unstable.maxInstanceId());
    }

    @Test
    void lastChosenValue() {
        Unstable unstable = new Unstable(new MemoryStorage());
        unstable.append(List.of(new ChosenValue(2), new ChosenValue(3),
                new ChosenValue(4)));
        assertEquals(4, unstable.lastChosenValue().getInstanceId());
        unstable.append(new ChosenValue(5));
        assertEquals(5, unstable.lastChosenValue().getInstanceId());
    }

}