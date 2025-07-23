// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.hubspotcrm.operation;

import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.entity.BasicHttpEntity;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.GroupingExpression;
import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.api.OAuth2Token;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.api.QueryRequest;
import com.boomi.connector.api.SimpleExpression;
import com.boomi.connector.api.Sort;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.hubspotcrm.HubspotcrmOperationConnection;
import com.boomi.connector.hubspotcrm.util.ExecutionUtils;
import com.boomi.util.TestUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Test class for HubspotcrmSearchOperation.
 * This class contains unit tests for validating the functionality of the HubSpot CRM search operations.
 */
public class HubspotcrmSearchOperationTest {

    HubspotcrmSearchOperation hubspotcrmSearchOperation;
    InputStream mockInputStream;
    @Mock
    private HubspotcrmOperationConnection mockConnection;
    @Mock
    private CloseableHttpClient mockHttpClient;
    @Mock
    private CloseableHttpResponse mockHttpResponse;
    @Mock
    private QueryRequest mockQueryRequest;
    @Mock
    private FilterData mockFilterData;
    @Mock
    private OperationResponse mockOperationResponse;
    @Mock
    private OperationContext mockOperationContext;
    @Mock
    private QueryFilter mockQueryFilter;

    @Mock
    private PropertyMap _connectionProperties;

    @Mock
    private OAuth2Token mockOAuth2Token;

    @Mock
    private OAuth2Context oAuthContextMock;

    @Mock
    private ObjectMapper mockObjectMapper;

    @Mock
    private ObjectNode mockObjectNode;

    @Mock
    private ArrayNode mockArrayNode;

    private static final String PROPERTIES = "properties";
    private static final String VALUES = "values";
    private static final String SORTS = "sorts";

    /**
     * Sets up the test environment before each test.
     * Initializes mocks and configures common behavior.
     *
     * @throws IOException if there's an error during setup
     */
    @Before
    public void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);
        Mockito.when(mockConnection.getContext()).thenReturn(mockOperationContext);
        Mockito.when(mockOperationContext.getConnectionProperties()).thenReturn(_connectionProperties);
        Mockito.when(mockConnection.getOAuthContext()).thenReturn(oAuthContextMock);
        hubspotcrmSearchOperation = new HubspotcrmSearchOperation(mockConnection);
        Mockito.when(mockOperationContext.getConnectionProperties().getOAuth2Context("oauthContext")).thenReturn(
                oAuthContextMock);
        Mockito.when(mockConnection.getOAuthContext().getOAuth2Token(false)).thenReturn(mockOAuth2Token);
        Mockito.when(mockOAuth2Token.getAccessToken()).thenReturn("testBearerToken");

        // Mock ObjectMapper behavior
        Mockito.when(mockObjectMapper.createObjectNode()).thenReturn(mockObjectNode);

        // Mock ArrayNode creation
        Mockito.when(mockObjectNode.putArray(PROPERTIES)).thenReturn(mockArrayNode);
        Mockito.when(mockObjectNode.putArray(VALUES)).thenReturn(mockArrayNode);
        // Mock group node creation
        Mockito.when(mockObjectNode.objectNode()).thenReturn(mockObjectNode);

        // Mock filters array creation
        Mockito.when(mockObjectNode.putArray("filters")).thenReturn(mockArrayNode);
        Mockito.when(mockArrayNode.add(mockObjectNode)).thenReturn(mockArrayNode);

        // Setup basic mocks
        Mockito.when(mockQueryRequest.getFilter()).thenReturn(mockFilterData);
        Mockito.when(mockObjectNode.putArray("properties")).thenReturn(mockArrayNode);

        List<String> selectedFields = new ArrayList<>();
        selectedFields.add("properties/about_us");
        selectedFields.add("properties/address");
        selectedFields.add("properties/country");

        Mockito.when(mockOperationContext.getSelectedFields()).thenReturn(selectedFields);
        Mockito.when(mockFilterData.getFilter()).thenReturn(mockQueryFilter);
        Mockito.when(mockOperationContext.getConnectionProperties()).thenReturn(_connectionProperties);
        Mockito.when(mockOperationContext.getObjectTypeId()).thenReturn("POST::/crm/v3/objects/contacts/search");
        Mockito.when(mockHttpClient.execute(Mockito.any())).thenReturn(mockHttpResponse);

        mockInputStream = new ByteArrayInputStream("{\"results\":[]}".getBytes());
        TestUtil.disableBoomiLog();
    }

    /**
     * Tests successful query execution with basic parameters.
     *
     * @throws Exception if the test fails
     */
    @Test
    public void testExecuteQuerySuccess() throws Exception {
        mockHttpResponse(200);
        prepareExecuteQueryMocks();
        executeQueryAndVerify();
        Mockito.verify(mockOperationResponse, Mockito.atLeastOnce()).finishPartialResult((TrackedData) Mockito.any());
    }

    /**
     * Tests query execution with sort fields.
     *
     * @throws Exception if the test fails
     */
    @Test
    public void testExecuteQuerySuccessWithSortField() throws Exception {
        mockHttpResponse(200);
        prepareExecuteQueryMocks();
        ArrayList<Sort> sortList = new ArrayList<>();
        Sort sort = new Sort();
        sort.setProperty("properties/city");
        sort.setSortOrder("DESCENDING");
        sortList.add(sort);
        Mockito.when(mockObjectNode.putArray(SORTS)).thenReturn(mockArrayNode);
        Mockito.when(mockQueryFilter.getSort()).thenReturn(sortList);
        Mockito.when(mockObjectNode.putArray(SORTS)).thenReturn(mockArrayNode);
        Mockito.when(mockArrayNode.addObject()).thenReturn(mockObjectNode);
        executeQueryAndVerify();
    }

    /**
     * Tests query execution with BETWEEN filter.
     *
     * @throws Exception if the test fails
     */
    @Test
    public void testExecuteQueryWithFilterEqual() throws Exception {
        mockHttpResponse(200);
        prepareExecuteQueryMocks();

        SimpleExpression expression = setupFilterExpression("EQ", "properties/country", "United State");
        QueryFilter filter = new QueryFilter();
        filter.setExpression(expression);

        GroupingExpression groupExpr = new GroupingExpression();
        filter.setExpression(groupExpr);
        Mockito.when(mockFilterData.getFilter()).thenReturn(filter);

        ObjectNode secondFilterNode = Mockito.mock(ObjectNode.class);
        Mockito.when(mockObjectMapper.createObjectNode()).thenReturn(mockObjectNode).thenReturn(mockObjectNode)
                .thenReturn(mockObjectNode).thenReturn(secondFilterNode);

        executeQueryAndVerify();
        Mockito.verify(mockOperationResponse).finishPartialResult(mockFilterData);
    }

    /**
     * Sets up filter expression for testing.
     *
     * @param operator the filter operator
     * @param property the property to filter on
     * @param argument the filter argument
     * @return SimpleExpression configured with the provided parameters
     */
    private SimpleExpression setupFilterExpression(String operator, String property, String argument) {
        SimpleExpression expression = Mockito.mock(SimpleExpression.class);
        Mockito.when(expression.getArguments()).thenReturn(Collections.singletonList(argument));
        Mockito.when(expression.getOperator()).thenReturn(operator);
        Mockito.when(expression.getProperty()).thenReturn(property);
        Mockito.when(mockQueryFilter.getExpression()).thenReturn(expression);
        return expression;
    }

    /**
     * Prepares common query execution mocks.
     */
    private void prepareExecuteQueryMocks() {
        Mockito.when(mockQueryRequest.getFilter()).thenReturn(mockFilterData);
        Mockito.when(mockFilterData.getDynamicOperationProperties()).thenReturn(Mockito.mock(DynamicPropertyMap.class));
        Mockito.when(mockFilterData.getDynamicOperationProperties().getProperty("limit")).thenReturn("1");
    }

    /**
     * Prepares common query execution mocks.
     */
    private void mockHttpResponse(int statusCode) throws Exception {
        BasicHttpEntity mockEntity = new BasicHttpEntity();
        mockEntity.setContent(mockInputStream);
        Mockito.when(mockHttpClient.execute(Mockito.any())).thenReturn(mockHttpResponse);
        Mockito.when(mockHttpResponse.getEntity()).thenReturn(mockEntity);
        Mockito.when(mockHttpResponse.getStatusLine()).thenReturn(Mockito.mock(StatusLine.class));
        Mockito.when(mockHttpResponse.getStatusLine().getStatusCode()).thenReturn(statusCode);
    }

    /**
     * Executes query and verifies common expectations.
     */
    private void executeQueryAndVerify() {
        try (MockedStatic<ExecutionUtils> mockedStatic = Mockito.mockStatic(ExecutionUtils.class)) {
            Mockito.when(ExecutionUtils.getObjectMapper()).thenReturn(mockObjectMapper);

            // Mock ObjectMapper behavior
            Mockito.when(mockObjectMapper.createObjectNode()).thenReturn(mockObjectNode);
            Mockito.when(mockObjectNode.putArray(PROPERTIES)).thenReturn(mockArrayNode);
            ObjectNode mockFilterNode = Mockito.mock(ObjectNode.class);
            Mockito.when(mockObjectNode.putArray("filters")).thenReturn(mockArrayNode);
            Mockito.when(mockObjectNode.putArray("sorts")).thenReturn(mockArrayNode);
            Mockito.when(mockArrayNode.addObject()).thenReturn(mockFilterNode);
            Mockito.when(mockArrayNode.add(mockObjectNode)).thenReturn(mockArrayNode);
            Mockito.when(mockObjectMapper.writeValueAsString(Mockito.any())).thenReturn(
                    "{\"mockedJson\":\"mockValue\"}");

            mockedStatic.when(() -> ExecutionUtils.execute(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(mockHttpResponse);
            hubspotcrmSearchOperation.executeQuery(mockQueryRequest, mockOperationResponse);
            Mockito.verify(mockOperationResponse).finishPartialResult(mockFilterData);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}