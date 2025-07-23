//Copyright (c) 2022 Boomi, Inc.
package com.boomi.connector.googlebq.operation.upsert.strategy;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.googlebq.connection.GoogleBqOperationConnection;
import com.boomi.connector.googlebq.operation.upsert.strategy.result.BaseStrategyResult;
import com.boomi.connector.googlebq.operation.upsert.strategy.result.ErrorResponseStrategyResult;
import com.boomi.connector.googlebq.resource.TableResource;
import com.boomi.connector.googlebq.util.StatusUtil;
import com.boomi.restlet.client.ResponseUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.restlet.data.Response;

import java.security.GeneralSecurityException;

/**
 * Class in charge of deleting a temporary table and add the response to
 */
public class DeleteStrategy extends BaseStrategy {

    private static final String ERROR_TABLE_DELETE = "Cannot delete table with id %s";
    private static final String TEMPORARY_TABLE_FOR_LOAD = "temporaryTableForLoad";
    private static final String DELETE = "delete";
    private static final String STATUS_NODE = "status";
    private static final String NAME_NODE = "name";
    private static final String DELETED_VALUE = "deleted";
    private final String _datasetId;
    private final TableResource _tableResource;

    public DeleteStrategy(GoogleBqOperationConnection connection) {
        super(connection);
        _tableResource = new TableResource(connection);
        _datasetId = connection.getDatasetId();
    }

    @Override
    public BaseStrategyResult executeService(ObjectData document) throws GeneralSecurityException {
        String tableId = document.getDynamicOperationProperties().getProperty(TEMPORARY_TABLE_FOR_LOAD);

        Response response = _tableResource.deleteTable(_datasetId, tableId);
        if (ResponseUtil.validateResponse(response)) {
            String code = StatusUtil.getStatus(response);
            return BaseStrategyResult.createDeleteResult(buildResponseNode(tableId), code);
        }
        return ErrorResponseStrategyResult.create(response, String.format(ERROR_TABLE_DELETE, tableId));
    }

    @Override
    public String getNodeName() {
        return DELETE;
    }

    private static JsonNode buildResponseNode(String tableId) {
        ObjectNode node = JSONUtil.newObjectNode();
        node.put(NAME_NODE, tableId);
        node.put(STATUS_NODE, DELETED_VALUE);
        return node;
    }
}