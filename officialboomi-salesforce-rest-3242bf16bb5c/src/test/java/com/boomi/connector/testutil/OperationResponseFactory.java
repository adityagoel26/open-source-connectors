// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.testutil;

public class OperationResponseFactory {

    public static SimpleOperationResponse get(SimpleTrackedData... input) {
        SimpleOperationResponse operationResponse = new SimpleOperationResponse();
        for (SimpleTrackedData data : input) {
            operationResponse.addTrackedData(data);
        }
        return operationResponse;
    }
}
