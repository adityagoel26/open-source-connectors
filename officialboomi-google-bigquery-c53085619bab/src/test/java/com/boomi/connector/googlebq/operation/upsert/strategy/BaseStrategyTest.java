// Copyright (c) 2022 Boomi, Inc.
package com.boomi.connector.googlebq.operation.upsert.strategy;

import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.googlebq.connection.GoogleBqOperationConnection;
import com.boomi.connector.googlebq.operation.upsert.PayloadResponseBuilder;
import com.boomi.connector.googlebq.resource.TableResource;
import com.boomi.connector.googlebq.util.JsonResponseUtil;
import com.boomi.connector.googlebq.util.StatusUtil;
import com.boomi.restlet.client.ResponseUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.restlet.data.Response;

import java.io.IOException;

import static com.boomi.util.StringUtil.EMPTY_STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { DeleteStrategy.class, ResponseUtil.class, JsonResponseUtil.class, StatusUtil.class })
public class BaseStrategyTest {

    private final GoogleBqOperationConnection _connection = mock(GoogleBqOperationConnection.class);
    private final ObjectData _document = mock(ObjectData.class, Mockito.RETURNS_MOCKS);
    private final Response _response = mock(Response.class);

    @Before
    public void setUp() throws Exception {
        mockDeleteStrategy();
    }

    @Test
    public void shouldCorrectlyExecuteStrategy() {
        when(ResponseUtil.validateResponse(_response)).thenReturn(true);

        PayloadResponseBuilder builder = new PayloadResponseBuilder();
        BaseStrategy strategy = new DeleteStrategy(_connection);

        boolean response = strategy.execute(builder, _document);
        assertTrue(response);
    }

    @Test
    public void shouldReturnStrategyResultWithErrorWhenDeleteStrategyFail() throws IOException {
        when(ResponseUtil.validateResponse(_response)).thenReturn(false);
        when(JsonResponseUtil.extractPayload(_response)).thenReturn(buildJsonNode());

        PayloadResponseBuilder builder = new PayloadResponseBuilder();
        BaseStrategy strategy = new DeleteStrategy(_connection);
        boolean response = strategy.execute(builder, _document);

        assertFalse(response);
        assertEquals("Error on step delete message from delete. ", builder.getMessage());
    }

    private void mockDeleteStrategy() throws Exception {

        TableResource tableResource = mock(TableResource.class);
        mockStatic(ResponseUtil.class);
        mockStatic(JsonResponseUtil.class);
        mockStatic(StatusUtil.class);
        whenNew(TableResource.class).withArguments(eq(_connection)).thenReturn(tableResource);
        when(_connection.getProjectId()).thenReturn(EMPTY_STRING);
        OperationContext operationContext = mock(OperationContext.class);
        when(_connection.getContext()).thenReturn(operationContext);
        when(operationContext.getOperationProperties()).thenReturn(mock(PropertyMap.class));

        DynamicPropertyMap map = mock(DynamicPropertyMap.class);
        when(_document.getDynamicOperationProperties()).thenReturn(map);
        when(map.getProperty("temporaryTableForLoad")).thenReturn("testTempTable");
        when(tableResource.deleteTable(anyString(), anyString())).thenReturn(_response);
        when(StatusUtil.getStatus(_response)).thenReturn("200");
    }

    private JsonNode buildJsonNode() {
        ObjectNode node = JSONUtil.newObjectNode();
        ObjectNode errorNode = JSONUtil.newObjectNode();
        errorNode.put("code", "code");
        errorNode.put("message", "message from delete");
        node.set("error", errorNode);
        return node;
    }
}