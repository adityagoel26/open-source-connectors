// Copyright (c) 2025 Boomi, LP

package com.boomi.snowflake.util;

import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A class that manages cookie-related data using three different categories of key-value pairs:
 * default values, metadata values, and table metadata values.
 */
public class TableDefaultAndMetaDataObject {

    private SortedMap<String, String> _defaultValues;
    private SortedMap<String, String> _metaDataValues;
    private SortedMap<String, String> _tableMetaDataValues;

    /**
     * Constructs a CookieObject with specified maps for default values, metadata values,
     * and table metadata values. A defensive copy of each map is stored internally.
     *
     * @param defaultValues         A sorted map containing default key-value pairs
     * @param metaDataValues       A sorted map containing metadata key-value pairs
     * @param tableMetaDataValues  A sorted map containing table metadata key-value pairs
     */
    public TableDefaultAndMetaDataObject(SortedMap<String, String> defaultValues,
                SortedMap<String, String> metaDataValues, SortedMap<String, String> tableMetaDataValues) {
        this._defaultValues = new TreeMap<>(defaultValues);
        this._metaDataValues = new TreeMap<>(metaDataValues);
        this._tableMetaDataValues = new TreeMap<>(tableMetaDataValues);
    }

    /**
     * Constructs an empty CookieObject with no initialized values.
     */
    public TableDefaultAndMetaDataObject() {
        this._defaultValues = new TreeMap<>();
        this._metaDataValues = new TreeMap<>();
        this._tableMetaDataValues = new TreeMap<>();
    }

    /**
     * Returns a copy of the map of default values.
     *
     * @return A SortedMap containing default key-value pairs
     */
    public SortedMap<String, String> getDefaultValues() {
        return new TreeMap<>(_defaultValues);
    }

    /**
     * Sets the map of default values. A defensive copy is stored internally.
     *
     * @param defaultValues A SortedMap containing default key-value pairs
     */
    public void setDefaultValues(SortedMap<String, String> defaultValues) {
        this._defaultValues = new TreeMap<>(defaultValues);
    }

    /**
     * Returns a copy of the map of metadata values.
     *
     * @return A SortedMap containing metadata key-value pairs
     */
    public SortedMap<String, String> getMetaDataValues() {
        return new TreeMap<>(_metaDataValues);
    }

    /**
     * Sets the map of metadata values. A defensive copy is stored internally.
     *
     * @param metaDataValues A SortedMap containing metadata key-value pairs
     */
    public void setMetaDataValues(SortedMap<String, String> metaDataValues) {
        this._metaDataValues = new TreeMap<>(metaDataValues);
    }

    /**
     * Returns a copy of the map of table metadata values.
     *
     * @return A SortedMap containing table metadata key-value pairs
     */
    public SortedMap<String, String> getTableMetaDataValues() {
        return new TreeMap<>(_tableMetaDataValues);
    }

    /**
     * Sets the map of table metadata values. A defensive copy is stored internally.
     *
     * @param tableMetaDataValues A SortedMap containing table metadata key-value pairs
     */
    public void setTableMetaDataValues(SortedMap<String, String> tableMetaDataValues) {
        this._tableMetaDataValues = new TreeMap<>(tableMetaDataValues);
    }
}
