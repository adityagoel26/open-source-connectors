// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.properties;

import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.connector.testutil.SimpleTrackedData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class OperationPropertiesTest {

    @Test
    void propertiesMustBeNullWhenNotSet() {
        PropertyMap propertyMap = new MutablePropertyMap();
        propertyMap.put("columnDelimiter", "");
        propertyMap.put("externalIdFieldName", "");
        propertyMap.put("lineEnding", "");
        propertyMap.put("object", "");
        propertyMap.put("operation", "");

        MutableDynamicPropertyMap dynamicOpProps = new MutableDynamicPropertyMap();
        dynamicOpProps.addProperty("assignmentRuleId", "");

        TrackedData data = new SimpleTrackedData(0, "data", Collections.emptyMap(), Collections.emptyMap(),
                dynamicOpProps);

        OperationProperties operationProperties = new OperationProperties(propertyMap, "UPSERT");
        operationProperties.initDynamicProperties(data);

        assertNull(operationProperties.getAssignmentRuleId());
        assertNull(operationProperties.getColumnDelimiter());
        assertNull(operationProperties.getExternalIdFieldName());
        assertNull(operationProperties.getLineEnding());
        assertNull(operationProperties.getSObject());
        assertNull(operationProperties.getBulkOperation());
    }

    public static Stream<Arguments> getOperationBoomiNameTestCases() {
        return Stream.of(arguments("UPDATE", "UPDATE"), //
                arguments("UPSERT", "UPSERT"), //
                arguments("CREATE", "CREATE"),//
                arguments("CREATE_TREE", "CREATE"), //
                arguments("QUERY", "QUERY"), //
                arguments("DELETE", "DELETE") //
        );
    }

    @ParameterizedTest
    @MethodSource("getOperationBoomiNameTestCases")
    void getOperationBoomiName(String operationName, String expectedOperationName) {
        OperationProperties operationProperties = new OperationProperties(new MutablePropertyMap(), operationName);
        assertEquals(expectedOperationName, operationProperties.getOperationBoomiName());
    }
}
