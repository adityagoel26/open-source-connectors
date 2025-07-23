// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.testutil;

public class SimpleOperationResponseWrapper {

    private SimpleOperationResponseWrapper(){}

    public static void  addTrackedData(SimpleTrackedData data, SimpleOperationResponse simpleOperationResponse) {

        simpleOperationResponse.addTrackedData(data);
    }

}
