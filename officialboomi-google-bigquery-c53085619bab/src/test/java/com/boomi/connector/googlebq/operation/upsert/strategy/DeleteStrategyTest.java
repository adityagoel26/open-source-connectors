// Copyright (c) 2022 Boomi, Inc.
package com.boomi.connector.googlebq.operation.upsert.strategy;

import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.googlebq.connection.GoogleBqOperationConnection;
import com.boomi.connector.googlebq.operation.upsert.strategy.result.BaseStrategyResult;
import com.boomi.connector.googlebq.resource.TableResource;
import com.boomi.connector.googlebq.util.JsonResponseUtil;
import com.boomi.restlet.client.ResponseUtil;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.restlet.data.Response;

import java.security.GeneralSecurityException;

import static com.boomi.util.StringUtil.EMPTY_STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { DeleteStrategy.class, ResponseUtil.class, JsonResponseUtil.class })
public class DeleteStrategyTest {

    private final DynamicPropertyMap _map = mock(DynamicPropertyMap.class);
    private final ObjectData _document = mock(ObjectData.class);
    private final TableResource _tableResource = mock(TableResource.class);
    private final GoogleBqOperationConnection _connection = mock(GoogleBqOperationConnection.class);
    private final Response _response = mock(Response.class);

    @Before
    public final void setup() throws Exception {
        mockStatic(ResponseUtil.class);
        mockStatic(JsonResponseUtil.class);
        whenNew(TableResource.class).withArguments(eq(_connection)).thenReturn(_tableResource);
        when(_connection.getProjectId()).thenReturn(EMPTY_STRING);
        OperationContext operationContext = mock(OperationContext.class);
        when(_connection.getContext()).thenReturn(operationContext);
        when(operationContext.getOperationProperties()).thenReturn(mock(PropertyMap.class));
        when(_map.getProperty("temporaryTableForLoad")).thenReturn("testTempTable");
        when(_document.getDynamicOperationProperties()).thenReturn(_map);
    }

    @Test
    public void shouldCorrectlyExecuteService() throws GeneralSecurityException {
        when(_tableResource.deleteTable(anyString(), anyString())).thenReturn(_response);
        when(ResponseUtil.validateResponse(_response)).thenReturn(true);

        DeleteStrategy strategy = new DeleteStrategy(_connection);
        BaseStrategyResult response = strategy.executeService(_document);

        assertNotNull(response);
        assertTrue(response.isSuccess());
        assertEquals("testTempTable", response.getContent().path("name").asText());
    }

    @Test
    public void shouldReturnErrorStrategyResultWhenDeleteTableFail() throws Exception {
        String errorMessage = "message from delete";
        when(_tableResource.deleteTable(anyString(), anyString())).thenReturn(_response);
        when(ResponseUtil.validateResponse(_response)).thenReturn(false);
        when(JsonResponseUtil.extractPayload(_response)).thenReturn(StrategyUtil.buildErrorJsonNode(errorMessage));

        DeleteStrategy strategy = new DeleteStrategy(_connection);
        BaseStrategyResult response = strategy.executeService(_document);
        assertNotNull(response);
        assertFalse(response.isSuccess());
        assertEquals(errorMessage, response.getErrorMessage());
    }

    @Test
    public void shouldReturnDeleteName() {
        DeleteStrategy strategy = new DeleteStrategy(_connection);
        assertEquals("delete", strategy.getNodeName());
    }
}