// Copyright (c) 2025 Boomi, LP

package com.boomi.snowflake.util;

import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.*;
import org.junit.Test;

public class DefaultValueObjectTest {

    /**
     * Test that the constructor creates defensive copies of the input maps.
     */
    @Test
    public void testCookieObjectConstructorDefensiveCopies() {
        SortedMap<String, String> defaultValues = new TreeMap<>();
        SortedMap<String, String> metaDataValues = new TreeMap<>();
        SortedMap<String, String> tableMetaDataValues = new TreeMap<>();

        defaultValues.put("key", "value");
        metaDataValues.put("key", "value");
        tableMetaDataValues.put("key", "value");

        TableDefaultAndMetaDataObject cookieObject = new TableDefaultAndMetaDataObject(defaultValues, metaDataValues, tableMetaDataValues);

        defaultValues.clear();
        metaDataValues.clear();
        tableMetaDataValues.clear();

        assertFalse(cookieObject.getDefaultValues().isEmpty());
        assertFalse(cookieObject.getMetaDataValues().isEmpty());
        assertFalse(cookieObject.getTableMetaDataValues().isEmpty());
    }

    @Test
    public void testCookieObjectConstructorDefensiveCopy() {
        /**
         * Test that the CookieObject constructor creates a defensive copy of the input maps,
         * ensuring that subsequent changes to the original maps do not affect the CookieObject.
         */
        SortedMap<String, String> defaultValues = new TreeMap<>();
        defaultValues.put("key", "value");

        TableDefaultAndMetaDataObject cookieObject = new TableDefaultAndMetaDataObject(defaultValues, new TreeMap<>(), new TreeMap<>());

        defaultValues.clear();

        assertEquals(1, cookieObject.getDefaultValues().size());
        assertEquals("value", cookieObject.getDefaultValues().get("key"));
    }

    @Test
    public void testCookieObjectConstructorWithEmptyMaps() {
        /**
         * Test that the CookieObject constructor accepts empty maps as valid input.
         */
        SortedMap<String, String> emptyMap = new TreeMap<>();
        TableDefaultAndMetaDataObject cookieObject = new TableDefaultAndMetaDataObject(emptyMap, emptyMap, emptyMap);

        assertTrue(cookieObject.getDefaultValues().isEmpty());
        assertTrue(cookieObject.getMetaDataValues().isEmpty());
        assertTrue(cookieObject.getTableMetaDataValues().isEmpty());
    }

    /**
     * Test the constructor of CookieObject with non-empty maps
     * Verifies that the constructor creates a defensive copy of the input maps
     */
    @Test
    public void testCookieObjectConstructorWithNonEmptyMaps() {
        // Arrange
        SortedMap<String, String> defaultValues = new TreeMap<>();
        defaultValues.put("default1", "value1");
        defaultValues.put("default2", "value2");

        SortedMap<String, String> metaDataValues = new TreeMap<>();
        metaDataValues.put("meta1", "value1");
        metaDataValues.put("meta2", "value2");

        SortedMap<String, String> tableMetaDataValues = new TreeMap<>();
        tableMetaDataValues.put("table1", "value1");
        tableMetaDataValues.put("table2", "value2");

        // Act
        TableDefaultAndMetaDataObject cookieObject = new TableDefaultAndMetaDataObject(defaultValues, metaDataValues, tableMetaDataValues);

        // Assert
        assertNotSame(defaultValues, cookieObject.getDefaultValues());
        assertNotSame(metaDataValues, cookieObject.getMetaDataValues());
        assertNotSame(tableMetaDataValues, cookieObject.getTableMetaDataValues());

        assertEquals(defaultValues, cookieObject.getDefaultValues());
        assertEquals(metaDataValues, cookieObject.getMetaDataValues());
        assertEquals(tableMetaDataValues, cookieObject.getTableMetaDataValues());

        // Modify original maps to ensure defensive copy
        defaultValues.put("default3", "value3");
        metaDataValues.put("meta3", "value3");
        tableMetaDataValues.put("table3", "value3");

        assertNotEquals(defaultValues, cookieObject.getDefaultValues());
        assertNotEquals(metaDataValues, cookieObject.getMetaDataValues());
        assertNotEquals(tableMetaDataValues, cookieObject.getTableMetaDataValues());
    }


    @Test
    public void testCookieObjectConstructorWithNullValues() {
        /**
         * Test that the CookieObject constructor accepts maps with null values.
         */
        SortedMap<String, String> mapWithNullValue = new TreeMap<>();
        mapWithNullValue.put("key", null);

        TableDefaultAndMetaDataObject cookieObject = new TableDefaultAndMetaDataObject(mapWithNullValue, new TreeMap<>(), new TreeMap<>());

        assertEquals(1, cookieObject.getDefaultValues().size());
        assertNull(cookieObject.getDefaultValues().get("key"));
    }

    /**
     * Test that the default constructor initializes empty maps.
     */
    @Test
    public void testCookieObjectDefaultConstructor() {
        TableDefaultAndMetaDataObject cookieObject = new TableDefaultAndMetaDataObject();

        assertTrue(cookieObject.getDefaultValues().isEmpty());
        assertTrue(cookieObject.getMetaDataValues().isEmpty());
        assertTrue(cookieObject.getTableMetaDataValues().isEmpty());
    }

    /**
     * Test the default constructor of CookieObject
     * Verifies that the constructor initializes empty maps for default values,
     * metadata values, and table metadata values.
     */

    @Test
    public void testGetDefaultValuesImmutability() {
        /**
         * Test that modifying the returned map does not affect the original map.
         * This tests the edge case of ensuring immutability of the internal state.
         */
        SortedMap<String, String> defaultValues = new TreeMap<>();
        defaultValues.put("key", "value");
        TableDefaultAndMetaDataObject cookieObject = new TableDefaultAndMetaDataObject(defaultValues, new TreeMap<>(), new TreeMap<>());

        SortedMap<String, String> result = cookieObject.getDefaultValues();
        result.put("newKey", "newValue");

        assertFalse(cookieObject.getDefaultValues().containsKey("newKey"));
    }

    @Test
    public void testGetDefaultValuesReturnsCopy() {
        /**
         * Test that getDefaultValues returns a copy of the map and not the original.
         * This tests the edge case of ensuring defensive copying.
         */
        SortedMap<String, String> defaultValues = new TreeMap<>();
        defaultValues.put("key", "value");
        TableDefaultAndMetaDataObject cookieObject = new TableDefaultAndMetaDataObject(defaultValues, new TreeMap<>(), new TreeMap<>());

        SortedMap<String, String> result = cookieObject.getDefaultValues();

        assertEquals(defaultValues, result);
        assertNotSame(defaultValues, result);
    }

    @Test
    public void testGetDefaultValuesWithEmptyMap() {
        /**
         * Test getDefaultValues when the default values map is empty.
         * This tests the scenario where input is empty.
         */
        TableDefaultAndMetaDataObject cookieObject = new TableDefaultAndMetaDataObject();
        SortedMap<String, String> result = cookieObject.getDefaultValues();

        assertTrue(result.isEmpty());
        assertNotSame(cookieObject.getDefaultValues(), result);
    }

    @Test
    public void testSetDefaultValuesDefensiveCopy() {
        /**
         * Test that setDefaultValues creates a defensive copy of the input map.
         * This test verifies that modifying the original map does not affect the internal state.
         */
        TableDefaultAndMetaDataObject cookieObject = new TableDefaultAndMetaDataObject();
        SortedMap<String, String> originalMap = new TreeMap<>();
        originalMap.put("key1", "value1");

        cookieObject.setDefaultValues(originalMap);
        originalMap.put("key2", "value2");

        SortedMap<String, String> retrievedMap = cookieObject.getDefaultValues();
        assertFalse(retrievedMap.containsKey("key2"));
    }

    @Test
    public void testSetDefaultValuesWithEmptyInput() {
        /**
         * Test setDefaultValues method with an empty SortedMap.
         * This test verifies that an empty map is correctly set as the default values.
         */
        TableDefaultAndMetaDataObject cookieObject = new TableDefaultAndMetaDataObject();
        SortedMap<String, String> emptyMap = new TreeMap<>();

        cookieObject.setDefaultValues(emptyMap);

        assertEquals(emptyMap, cookieObject.getDefaultValues());
    }

    @Test
    public void testSetDefaultValuesWithLargeInput() {
        /**
         * Test setDefaultValues method with a large number of entries.
         * This test verifies that a large map is correctly set as the default values.
         */
        TableDefaultAndMetaDataObject cookieObject = new TableDefaultAndMetaDataObject();
        SortedMap<String, String> largeMap = new TreeMap<>();
        for (int i = 0; i < 10000; i++) {
            largeMap.put("key" + i, "value" + i);
        }

        cookieObject.setDefaultValues(largeMap);

        assertEquals(largeMap, cookieObject.getDefaultValues());
    }


    @Test
    public void testSetDefaultValuesWithSpecialCharacters() {
        /**
         * Test setDefaultValues method with keys and values containing special characters.
         * This test verifies that special characters are handled correctly.
         */
        TableDefaultAndMetaDataObject cookieObject = new TableDefaultAndMetaDataObject();
        SortedMap<String, String> specialMap = new TreeMap<>();
        specialMap.put("key!@#$%^&*()", "value!@#$%^&*()");
        specialMap.put("null", null);

        cookieObject.setDefaultValues(specialMap);

        assertEquals(specialMap, cookieObject.getDefaultValues());
    }

    @Test
    public void testSetMetaDataValuesImmutability() {
        /**
         * Test that the internal map is not affected by changes to the input map after setting.
         * This tests the immutability of the internal state after setting metadata values.
         */
        TableDefaultAndMetaDataObject cookieObject = new TableDefaultAndMetaDataObject();
        SortedMap<String, String> inputMap = new TreeMap<>();
        inputMap.put("key1", "value1");
        cookieObject.setMetaDataValues(inputMap);

        inputMap.put("key2", "value2");
        assertFalse(cookieObject.getMetaDataValues().containsKey("key2"));
    }

    /**
     * Test that setMetaDataValues correctly sets and stores a defensive copy of the provided metadata values.
     */
    @Test
    public void testSetMetaDataValuesStoresDefensiveCopy() {
        // Arrange
        TableDefaultAndMetaDataObject cookieObject = new TableDefaultAndMetaDataObject();
        SortedMap<String, String> initialMetaDataValues = new TreeMap<>();
        initialMetaDataValues.put("key1", "value1");
        initialMetaDataValues.put("key2", "value2");

        // Act
        cookieObject.setMetaDataValues(initialMetaDataValues);

        // Assert
        SortedMap<String, String> retrievedMetaDataValues = cookieObject.getMetaDataValues();

        // Verify that the retrieved map is equal to the initial map
        assertEquals(initialMetaDataValues, retrievedMetaDataValues);

        // Modify the original map
        initialMetaDataValues.put("key3", "value3");

        // Verify that the modification doesn't affect the internal state of CookieObject
        assertNotEquals(initialMetaDataValues, cookieObject.getMetaDataValues());

        // Modify the retrieved map
        retrievedMetaDataValues.put("key4", "value4");

        // Verify that modifying the retrieved map doesn't affect the internal state of CookieObject
        assertNotEquals(retrievedMetaDataValues, cookieObject.getMetaDataValues());
    }

    @Test
    public void testSetMetaDataValuesWithEmptyMap() {
        /**
         * Test setMetaDataValues with an empty map.
         * This tests the scenario where the input is empty.
         */
        TableDefaultAndMetaDataObject cookieObject = new TableDefaultAndMetaDataObject();
        SortedMap<String, String> emptyMap = new TreeMap<>();
        cookieObject.setMetaDataValues(emptyMap);
        assertTrue(cookieObject.getMetaDataValues().isEmpty());
    }


    @Test
    public void testSetMetaDataValuesWithNullValue() {
        /**
         * Test setMetaDataValues with a map containing a null value.
         * This tests the scenario where the input is invalid (null value).
         */
        TableDefaultAndMetaDataObject cookieObject = new TableDefaultAndMetaDataObject();
        SortedMap<String, String> mapWithNullValue = new TreeMap<>();
        mapWithNullValue.put("key", null);
        cookieObject.setMetaDataValues(mapWithNullValue);
        assertNull(cookieObject.getMetaDataValues().get("key"));
    }

    @Test
    public void testSetTableMetaDataValuesDefensiveCopy() {
        /**
         * Test that setTableMetaDataValues creates a defensive copy
         * This tests an edge case to ensure the internal state is not affected by external changes
         */
        TableDefaultAndMetaDataObject cookieObject = new TableDefaultAndMetaDataObject();
        SortedMap<String, String> originalMap = new TreeMap<>();
        originalMap.put("key", "value");
        cookieObject.setTableMetaDataValues(originalMap);

        // Modify the original map
        originalMap.put("newKey", "newValue");

        // The internal map should not be affected
        assertFalse(cookieObject.getTableMetaDataValues().containsKey("newKey"));
    }


    @Test
    public void testSetTableMetaDataValuesWithEmptyInput() {
        /**
         * Test setTableMetaDataValues with empty input
         * This tests the scenario where the input is empty
         */
        TableDefaultAndMetaDataObject cookieObject = new TableDefaultAndMetaDataObject();
        SortedMap<String, String> emptyMap = new TreeMap<>();
        cookieObject.setTableMetaDataValues(emptyMap);
        assertTrue(cookieObject.getTableMetaDataValues().isEmpty());
    }


}