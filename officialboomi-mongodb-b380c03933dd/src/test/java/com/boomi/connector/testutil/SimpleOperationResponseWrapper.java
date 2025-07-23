// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.testutil;

public class SimpleOperationResponseWrapper {

    public void addTrackedData(SimpleTrackedData data, SimpleOperationResponse simpleOperationResponse) {

        simpleOperationResponse.addTrackedData(data);
    }
}