// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.operation.bulkv2.xml;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PairTest {

    @Test
    void gettersTest() {
        Pair<String, Integer> pair = new Pair<>("test", 2);
        assertEquals("test", pair.getKey());
        assertEquals(2, pair.getValue());
    }
}
