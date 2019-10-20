package com.yuyuko.paxoskv.core.storage;

import com.yuyuko.paxoskv.core.ChosenValue;
import com.yuyuko.paxoskv.core.PaxosException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.function.ThrowingSupplier;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.of;

class MemoryStorageTest {

    @Test
    void append() {
        MemoryStorage memoryStorage = new MemoryStorage();
        memoryStorage.append(List.of(new ChosenValue(0)));
        assertEquals(0, memoryStorage.maxInstanceId());
        memoryStorage.append(List.of(new ChosenValue(0)));
        assertEquals(0, memoryStorage.maxInstanceId());
        memoryStorage.append(List.of(new ChosenValue(0), new ChosenValue(1)));
        assertEquals(1, memoryStorage.maxInstanceId());
        memoryStorage.append(List.of(new ChosenValue(0), new ChosenValue(1), new ChosenValue(2)));
        assertEquals(2, memoryStorage.maxInstanceId());
        memoryStorage.append(List.of(new ChosenValue(3)));
        assertEquals(3, memoryStorage.maxInstanceId());
        memoryStorage.append(List.of(new ChosenValue(2)));
        assertEquals(3, memoryStorage.maxInstanceId());
        memoryStorage.append(List.of(new ChosenValue(2),new ChosenValue(3),new ChosenValue(4)));
        assertEquals(4, memoryStorage.maxInstanceId());
    }

    @Test
    void append2() {
        MemoryStorage memoryStorage = new MemoryStorage();
        memoryStorage.append(List.of(new ChosenValue(1)));
        assertEquals(1, memoryStorage.maxInstanceId());
        memoryStorage.append(List.of(new ChosenValue(0), new ChosenValue(1), new ChosenValue(2)));
        assertEquals(2, memoryStorage.maxInstanceId());
        memoryStorage.append(List.of(new ChosenValue(3)));
        assertEquals(3, memoryStorage.maxInstanceId());
        memoryStorage.append(List.of(new ChosenValue(2)));
        assertEquals(3, memoryStorage.maxInstanceId());
        memoryStorage.append(List.of(new ChosenValue(2),new ChosenValue(3),new ChosenValue(4)));
        assertEquals(4, memoryStorage.maxInstanceId());

        assertThrows(PaxosException.class, () -> memoryStorage.append(List.of(new ChosenValue(6))));
    }

    @ParameterizedTest
    @MethodSource("getGenerator")
    void get(long instanceId, boolean wEx) {
        MemoryStorage memoryStorage = new MemoryStorage();
        memoryStorage.append(List.of(new ChosenValue(1), new ChosenValue(2), new ChosenValue(3)));

        Executable supplier =
                () -> memoryStorage.get(instanceId);
        if (wEx)
            assertThrows(DataNotFoundException.class, supplier);
        else
            assertDoesNotThrow(supplier);
    }

    static Stream<Arguments> getGenerator() {
        return Stream.of(of(0L, true), of(1L, false), of(4L, true));
    }

    @ParameterizedTest
    @MethodSource("listFromGenerator")
    void listFrom(long from, boolean wEx, int size) {
        MemoryStorage memoryStorage = new MemoryStorage();
        memoryStorage.append(List.of(new ChosenValue(1), new ChosenValue(2), new ChosenValue(3)));
        boolean hasEx = false;

        try {
            List<ChosenValue> list = memoryStorage.list(from);
            assertEquals(size, list.size());
        } catch (DataNotFoundException ex) {
            hasEx = true;
        }
        if (hasEx ^ wEx)
            fail();
    }

    static Stream<Arguments> listFromGenerator() {
        return Stream.of(of(1L, false, 3), of(0L, true, 0), of(2L, false, 2), of(3L, false, 1),
                of(4L, true, 0));
    }

    @ParameterizedTest
    @MethodSource("listBetweenGenerator")
    void listBetween(long from, long to, boolean wEx, int size) {
        MemoryStorage memoryStorage = new MemoryStorage();
        memoryStorage.append(List.of(new ChosenValue(1), new ChosenValue(2), new ChosenValue(3)));

        boolean hasEx = false;

        try {
            List<ChosenValue> list = memoryStorage.list(from, to);
            assertEquals(size, list.size());
        } catch (DataNotFoundException ex) {
            hasEx = true;
        }
        if (hasEx ^ wEx)
            fail();
    }

    static Stream<Arguments> listBetweenGenerator() {
        return Stream.of(of(1L, 0L, true, 0), of(0L, 1L, true, 0), of(1L, 3L, false, 2),
                of(1L, 4L, false, 3), of(0L, 4L, true, 0), of(4L, 4L, true, 0));
    }

    @Test
    void maxInstanceId() {
        MemoryStorage storage = new MemoryStorage();
        assertThrows(DataNotFoundException.class, storage::maxInstanceId);
        storage.append(List.of(new ChosenValue(0)));
        assertEquals(0, storage.maxInstanceId());
    }
}