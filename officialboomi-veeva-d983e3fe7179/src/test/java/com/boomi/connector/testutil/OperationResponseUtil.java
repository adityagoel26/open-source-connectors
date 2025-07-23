// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.testutil;

public class OperationResponseUtil {

    public static void addSimpleTrackedData(SimpleOperationResponse response, SimpleTrackedData data) {
        response.addTrackedData(data);
    }
}