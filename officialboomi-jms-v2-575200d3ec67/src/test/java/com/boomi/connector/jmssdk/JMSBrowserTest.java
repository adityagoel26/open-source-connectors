// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.ConnectionTester;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.ui.BrowseField;
import com.boomi.connector.api.ui.DataType;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.pool.AdapterFactory;
import com.boomi.util.CollectionUtil;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JMSBrowserTest {

    private static final VerificationMode INVOKED_ONCE = Mockito.times(1);

    @Test
    @SuppressWarnings("unchecked")
    public void testConnectionShouldCloseAdapterAfterExecutingTest() {
        JMSConnection<BrowseContext> connectionMock = mock(JMSConnection.class);
        AdapterFactory adapterFactoryMock =  mock(AdapterFactory.class);

        // the mocked connection returns a mocked adapter
        GenericJndiBaseAdapter adapterMock = mock(GenericJndiBaseAdapter.class);
        Mockito.when(adapterFactoryMock.makeObject()).thenReturn(adapterMock);

        ConnectionTester browser = new JMSBrowser(connectionMock, adapterFactoryMock);
        browser.testConnection();

        // verify the adapter was closed by the browser
        Mockito.verify(adapterMock, INVOKED_ONCE).close();
    }

    @Test(expected = ConnectorException.class)
    @SuppressWarnings("unchecked")
    public void testConnectionShouldThrowWhenAdapterCannotBeCreatedTest() {
        JMSConnection<BrowseContext> connectionMock = mock(JMSConnection.class);
        AdapterFactory adapterFactoryMock =  mock(AdapterFactory.class);

        // the mocked connection throws an error when asked for the adapter
        Mockito.when(adapterFactoryMock.makeObject()).thenThrow(RuntimeException.class);

        ConnectionTester browser = new JMSBrowser(connectionMock, adapterFactoryMock);
        browser.testConnection();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void dynamicDestinationObjectTypeTest() {

        JMSConnection jmsConnectionMock = mock(JMSConnection.class);
        AdapterFactory adapterFactoryMock = mock(AdapterFactory.class);
        GenericJndiBaseAdapter genericAdapterMock = mock(GenericJndiBaseAdapter.class);
        when(adapterFactoryMock.makeObject()).thenReturn(genericAdapterMock);

        Browser browser = new JMSBrowser(jmsConnectionMock,adapterFactoryMock);

        ObjectTypes objectTypes = browser.getObjectTypes();
        List<ObjectType> types = objectTypes.getTypes();

        Assert.assertEquals(1, types.size());

        ObjectType objectType = CollectionUtil.getFirst(types);

        Assert.assertEquals("dynamic_destination", objectType.getId());
        Assert.assertEquals("Dynamic Destination", objectType.getLabel());
    }

    @Test
    public void getObjectDefinitionsForSendOperation() {
        // prepare browser
        JMSTestContext context =
                new JMSTestContext.Builder().withGenericService().withPoolDisabled().withVersion2().build();
        context.setOperationType(OperationType.CREATE);
        context.setOperationCustomType("SEND");
        Browser browser = new JMSBrowser(new JMSConnection<BrowseContext>(context));

        // get definitions
        ObjectDefinitions objectDefinitions = browser.getObjectDefinitions("dynamic_destination",
                Arrays.asList(ObjectDefinitionRole.INPUT, ObjectDefinitionRole.OUTPUT));

        // validations
        List<ObjectDefinition> definitions = objectDefinitions.getDefinitions();
        Assert.assertEquals("there should be exactly 2 object definitions", 2, definitions.size());

        ObjectDefinition inputDefinition;
        ObjectDefinition outputDefinition;
        if (definitions.get(0).getInputType() == ContentType.BINARY) {
            inputDefinition = definitions.get(0);
            outputDefinition = definitions.get(1);
        } else {
            inputDefinition = definitions.get(1);
            outputDefinition = definitions.get(0);
        }

        // validate input definition
        Assert.assertEquals("expected binary content type for input definition", ContentType.BINARY,
                inputDefinition.getInputType());
        Assert.assertEquals("expected none type for input definition", ContentType.NONE,
                inputDefinition.getOutputType());
        Assert.assertNull("input definition should not have any json schema", inputDefinition.getJsonSchema());

        // validate output definition
        Assert.assertEquals(ContentType.NONE, outputDefinition.getInputType());
        Assert.assertEquals(ContentType.JSON, outputDefinition.getOutputType());
        Assert.assertNotNull(outputDefinition.getJsonSchema());

        List<BrowseField> importableFields = objectDefinitions.getOperationFields();
        Assert.assertEquals(2, importableFields.size());

        BrowseField destination = null;
        BrowseField destinationType = null;
        for (BrowseField field : importableFields) {
            if ("destination".equals(field.getId())) {
                destination = field;
            }
            if ("destination_type".equals(field.getId())) {
                destinationType = field;
            }
        }

        assertDestinationField(destination);
        Assert.assertEquals("Indicate the destination where messages will be sent.", destination.getHelpText());
        assertDestinationTypeField(destinationType);
    }

    @Test
    public void getObjectDefinitionsForGetOperation() {
        // prepare browser
        JMSTestContext context =
                new JMSTestContext.Builder().withGenericService().withPoolDisabled().withVersion2().build();
        context.setOperationType(OperationType.QUERY);
        context.setOperationCustomType("GET");
        Browser browser = new JMSBrowser(new JMSConnection<BrowseContext>(context));

        // get definitions
        ObjectDefinitions objectDefinitions = browser.getObjectDefinitions("dynamic_destination",
                Arrays.asList(ObjectDefinitionRole.INPUT, ObjectDefinitionRole.OUTPUT));

        // validations
        List<ObjectDefinition> definitions = objectDefinitions.getDefinitions();
        Assert.assertEquals("there should be exactly 2 object definitions", 2, definitions.size());

        ObjectDefinition inputDefinition;
        ObjectDefinition outputDefinition;
        if (definitions.get(0).getInputType() == ContentType.BINARY) {
            outputDefinition = definitions.get(0);
            inputDefinition = definitions.get(1);
        } else {
            outputDefinition = definitions.get(1);
            inputDefinition = definitions.get(0);
        }

        // validate input definition
        Assert.assertEquals("expected none content type for input definition's output", ContentType.NONE,
                inputDefinition.getOutputType());
        Assert.assertEquals("expected none content type for input definition's input", ContentType.NONE,
                inputDefinition.getInputType());

        // validate output definition
        Assert.assertEquals("expected none content type for output definition's input", ContentType.NONE,
                outputDefinition.getInputType());
        Assert.assertEquals("expected none content type for output definition's output", ContentType.BINARY,
                outputDefinition.getOutputType());

        List<BrowseField> importableFields = objectDefinitions.getOperationFields();
        Assert.assertEquals(1, importableFields.size());

        BrowseField destination = CollectionUtil.getFirst(importableFields);
        assertDestinationField(destination);
        Assert.assertEquals("Indicate the destination from where to retrieve the messages.", destination.getHelpText());
    }

    private static void assertDestinationField(BrowseField field) {
        Assert.assertNotNull("destination should not be null", field);
        Assert.assertEquals("destination", field.getId());
        Assert.assertEquals("Destination", field.getLabel());
        Assert.assertEquals(DataType.STRING, field.getType());
        Assert.assertEquals(0, field.getAllowedValues().size());
        Assert.assertNull(field.getDefaultValue());
        Assert.assertTrue("destination field should be overridable", field.isOverrideable());
    }

    private static void assertDestinationTypeField(BrowseField field) {
        Assert.assertNotNull("destination_type should not be null", field);
        Assert.assertEquals("destination_type", field.getId());
        Assert.assertEquals("Destination Type", field.getLabel());
        Assert.assertEquals(DataType.STRING, field.getType());
        Assert.assertEquals(4, field.getAllowedValues().size());
        Assert.assertEquals("text_message", field.getDefaultValue());
        Assert.assertEquals("Indicate the destination type.", field.getHelpText());
        Assert.assertTrue("destination type field should be overridable", field.isOverrideable());
    }
}
