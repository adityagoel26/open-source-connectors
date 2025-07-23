// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.mongodb.util;

import com.boomi.connector.api.*;
import com.boomi.connector.mongodb.MongoDBConnectorConnection;
import com.boomi.connector.mongodb.actions.RetryableUpdateUpsertOperation;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class UpdateUpsertUtil {

    private UpdateUpsertUtil (){
        throw new IllegalStateException("Utility class");
    }

    public static void executeUpdateUpsertOperation(OperationContext operationContext,
                                                    MongoDBConnectorConnection connectorConnection,
                                                    UpdateRequest request,
                                                    OperationResponse response) {
        Logger responseLogger = response.getLogger();
        List<ObjectData> trackedData = new ArrayList<>();
        for(ObjectData objData : request) {
            trackedData.add(objData);
        }

        try {
            Map<String, Object> inputConfig = connectorConnection.prepareInputConfig(operationContext, responseLogger);
            RetryableUpdateUpsertOperation operation = new RetryableUpdateUpsertOperation(connectorConnection,
                    operationContext.getObjectTypeId(), response, inputConfig, operationContext.getConfig(), trackedData,
                    StandardCharsets.UTF_8);
            operation.execute();
        }catch(Exception e) {
            ResponseUtil.addExceptionFailures(response, trackedData, e);
        }
        finally {
            connectorConnection.closeConnection();
        }
    }
}
