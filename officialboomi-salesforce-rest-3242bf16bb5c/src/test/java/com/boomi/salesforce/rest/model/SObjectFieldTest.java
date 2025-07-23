// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SObjectFieldTest {

    @Test
    void getIntegerPlaces() {
        SObjectField field = new SObjectField("field", "integer", 11, 5);
        Assertions.assertEquals(5, field.getScale());
        Assertions.assertEquals(6, field.getIntegerPlaces());
    }

    @Test
    void getPlatformFormat() {
        SObjectField field = new SObjectField("field", "integer", 12, 5);
        Assertions.assertEquals("#######.#####", field.getPlatformFormat());
    }

    @Test
    void getNullPlatformFormat() {
        SObjectField field = new SObjectField("field", "integer");
        Assertions.assertNull(field.getPlatformFormat(), "null is expected when no precision or scale is provided");
    }
}
