// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.operation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.api.QueryRequest;
import com.boomi.connector.api.Sort;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.connector.testutil.QueryFilterBuilder;
import com.boomi.connector.testutil.QueryGroupingBuilder;
import com.boomi.connector.testutil.QuerySimpleBuilder;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.constant.OperationAPI;
import com.boomi.salesforce.rest.controller.operation.QueryController;
import com.boomi.salesforce.rest.model.SObjectModel;
import com.boomi.salesforce.rest.properties.ConnectionProperties;
import com.boomi.salesforce.rest.properties.OperationProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SFRestQueryTest {

    private static final String CORRECT_SOQL =
            "SELECT Name,Type,Owner.Email,(SELECT Name FROM Contacts) FROM Account WHERE (Name like '%a%' AND Type "
                    + "like '%a%') ORDER BY Name ASC NULLS LAST LIMIT 10";
    private static final String CORRECT_SOQL_NO_FILTER =
            "SELECT Name,Type,Owner.Email,(SELECT Name FROM Contacts) FROM Account ORDER BY Name ASC NULLS LAST LIMIT"
                    + " 10";
    private static final String MESSAGE = "Message";
    private static final List<String> SELECTED_FIELDS = Arrays.asList("Name", "Type", "Contacts/records/Name",
            "Owner/Email");
    private static final String SF_OBJECT_ACCOUNT = "Account";
    private static final String OPERATION_NAME_QUERY = "QUERY";
    private final OperationResponse _operationResponse = mock(OperationResponse.class);
    private final QueryRequest _updateRequest = mock(QueryRequest.class);
    private final SFRestConnection _connection = mock(SFRestConnection.class, Mockito.RETURNS_DEEP_STUBS);
    private final ConnectionProperties _connectionProperties = mock(ConnectionProperties.class);
    private final ObjectData _objectData = mock(ObjectData.class);
    private final MutablePropertyMap _platformProperties = new MutablePropertyMap();
    private OperationProperties _operationProperties;
    private MutableDynamicPropertyMap _overridableOperationProperties;

    @BeforeEach
    public void init() {
        _operationProperties = new OperationProperties(_platformProperties, OPERATION_NAME_QUERY);
        _overridableOperationProperties = new MutableDynamicPropertyMap();
        when(_connection.getOperationProperties()).thenReturn(_operationProperties);
        when(_objectData.getDynamicOperationProperties()).thenReturn(_overridableOperationProperties);
        when(_connection.getConnectionProperties()).thenReturn(_connectionProperties);

        _platformProperties.put(Constants.OBJECT_SOBJECT, SF_OBJECT_ACCOUNT);
        _platformProperties.put(OperationAPI.FIELD_ID, "RESTAPI");
    }

    @Test
    public void shouldCallGetConnection() {
        assertNotNull(new SFRestQueryOperation(_connection).getConnection());
    }

    @Test
    public void shouldCloseConnectionWhenInitializeThrowConnectorException() {
        Mockito.doThrow(new ConnectorException(MESSAGE)).when(_connection).initialize(any());
        try {
            Assertions.assertThrows(ConnectorException.class, () ->
            new SFRestQueryOperation(_connection).executeQuery(_updateRequest, _operationResponse));
        } finally {
            verify(_connection).close();
        }
    }

    @Test
    public void shouldGenerateCorrectSOQLWhenGivenQuerySortsAndSelectedFields() {
        QueryFilter queryFilter = new QueryFilterBuilder().toFilter();
        queryFilter.withSort(buildSort());

        _platformProperties.put(Constants.LIMIT_NUMBER_OF_DOCUMENTS_DESCRIPTOR, true);
        _overridableOperationProperties.addProperty(Constants.NUMBER_OF_DOCUMENTS_DESCRIPTOR, 10L);
        _operationProperties.initDynamicProperties(_objectData);

        QueryController controller = new QueryController(_connection);
        String soql = controller.buildQueryString(SELECTED_FIELDS, queryFilter);

        assertEquals(CORRECT_SOQL_NO_FILTER, soql);
    }

    @Test
    public void shouldGenerateCorrectSOQLWhenGivenLimitDocumentProperty() {
        QueryFilter queryFilter = new QueryFilterBuilder().toFilter();
        queryFilter.withSort(buildSort());

        _platformProperties.put(Constants.LIMIT_NUMBER_OF_DOCUMENTS_DESCRIPTOR, true);
        _overridableOperationProperties.addProperty(Constants.NUMBER_OF_DOCUMENTS_DESCRIPTOR, "10");

        _operationProperties.initDynamicProperties(_objectData);

        QueryController controller = new QueryController(_connection);
        String soql = controller.buildQueryString(SELECTED_FIELDS, queryFilter);

        assertEquals(CORRECT_SOQL_NO_FILTER, soql);
    }

    @Test
    public void shouldGenerateCorrectSOQLWhenGivenLimitOverridableOperationPropertyOverOperationProperty() {
        QueryFilter queryFilter = new QueryFilterBuilder().toFilter();
        queryFilter.withSort(buildSort());

        _platformProperties.put(Constants.LIMIT_NUMBER_OF_DOCUMENTS_DESCRIPTOR, true);
        _overridableOperationProperties.addProperty(Constants.NUMBER_OF_DOCUMENTS_DESCRIPTOR, "10");

        _operationProperties.initDynamicProperties(_objectData);

        QueryController controller = new QueryController(_connection);
        String soql = controller.buildQueryString(SELECTED_FIELDS, queryFilter);

        assertEquals(CORRECT_SOQL_NO_FILTER, soql);
    }

    @Test
    public void shouldGenerateCorrectSOQLWhenGivenQueryFilterAndSortsAndSelectedFields() {
        QueryFilter queryFilter = new QueryFilterBuilder(
                QueryGroupingBuilder.and(new QuerySimpleBuilder("Name", "LIKE", "%a%"),
                        new QuerySimpleBuilder("Type", "LIKE", "%a%"))).toFilter();
        queryFilter.withSort(buildSort());

        _platformProperties.put(Constants.LIMIT_NUMBER_OF_DOCUMENTS_DESCRIPTOR, true);
        _overridableOperationProperties.addProperty(Constants.NUMBER_OF_DOCUMENTS_DESCRIPTOR, 10L);
        _operationProperties.initDynamicProperties(_objectData);

        SObjectModel sObjectModel = new SObjectModel(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        when(_connectionProperties.isSObjectCached(SF_OBJECT_ACCOUNT, OPERATION_NAME_QUERY, false)).thenReturn(true);
        when(_connectionProperties.getSObject(SF_OBJECT_ACCOUNT, OPERATION_NAME_QUERY, false)).thenReturn(sObjectModel);

        QueryController controller = new QueryController(_connection);
        String soql = controller.buildQueryString(SELECTED_FIELDS, queryFilter);

        assertEquals(CORRECT_SOQL, soql);
    }

    private static Sort buildSort() {
        Sort sort = new Sort();
        sort.setSortOrder("asc_nulls_last");
        sort.setProperty("Name");
        return sort;
    }
}
