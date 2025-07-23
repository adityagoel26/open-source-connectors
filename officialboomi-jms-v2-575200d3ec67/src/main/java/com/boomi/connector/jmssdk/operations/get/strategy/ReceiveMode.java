// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get.strategy;

public enum ReceiveMode {
    NO_WAIT,
    LIMITED_NUMBER_OF_MESSAGES,
    LIMITED_NUMBER_OF_MESSAGES_WITH_TIMEOUT,
    UNLIMITED_NUMBER_OF_MESSAGES_WITH_TIMEOUT
}
