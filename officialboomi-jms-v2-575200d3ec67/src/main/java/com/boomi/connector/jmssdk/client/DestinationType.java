// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client;

import com.boomi.util.StringUtil;

/**
 * Enum representing the available data types for JMS Queues & Topics
 */
public enum DestinationType {
    TEXT_MESSAGE, TEXT_MESSAGE_XML, MAP_MESSAGE, BYTE_MESSAGE, ADT_MESSAGE;

    public static DestinationType fromValue(String value) {
        return valueOf(StringUtil.toUpperCase(value));
    }
}
