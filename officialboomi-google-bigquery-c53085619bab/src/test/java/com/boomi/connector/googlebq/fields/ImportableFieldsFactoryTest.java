package com.boomi.connector.googlebq.fields;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ui.BrowseField;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.connector.googlebq.resource.TableResource;
import com.boomi.util.TestUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { ImportableFieldsFactory.class })
public class ImportableFieldsFactoryTest {

    private final GoogleBqBaseConnection<BrowseContext> _connection = mock(GoogleBqBaseConnection.class);
    private final TableResource _tableResource = mock(TableResource.class);

    @BeforeClass
    public static void disableLogs() {
        TestUtil.disableBoomiLog();
    }

    @AfterClass
    public static void enableLogs() {
        TestUtil.restoreLog();
    }

    @Test
    public void shouldCreateBrowseFieldsWithDefaultValues() {
        ImportableFieldsFactory factory = new ImportableFieldsFactory(_connection);
        Collection<BrowseField> browseFields = factory.importableFields("GENERIC");

        assertNotNull(browseFields);
        assertEquals(8, browseFields.size());
        BrowseField destinationTableForLoad = getField(browseFields, "destinationTableForLoad");
        BrowseField temporaryTableForLoad = getField(browseFields, "temporaryTableForLoad");
        BrowseField tableSchema = getField(browseFields, "tableSchema");
        BrowseField sqlCommand = getField(browseFields, "sqlCommand");
        BrowseField targetTableForQuery = getField(browseFields, "targetTableForQuery");

        assertNull(destinationTableForLoad.getDefaultValue());
        assertNull(temporaryTableForLoad.getDefaultValue());
        assertNull(tableSchema.getDefaultValue());
        assertNull(sqlCommand.getDefaultValue());
        assertNull(targetTableForQuery.getDefaultValue());
    }

    @Test
    public void shouldCreateBrowseFieldsWithSchemaValues() throws Exception {
        ImportableFieldsFactory factory = new ImportableFieldsFactory(_connection);

        when(_tableResource.getTable(anyString())).thenReturn(buildSchemaJsonNode());
        whenNew(TableResource.class).withArguments(_connection).thenReturn(_tableResource);
        Collection<BrowseField> browseFields = factory.importableFields("testproject/datatest/tabletest");

        assertNotNull(browseFields);
        assertEquals(8, browseFields.size());

        BrowseField destinationTableForLoad = getField(browseFields, "destinationTableForLoad");
        BrowseField temporaryTableForLoad = getField(browseFields, "temporaryTableForLoad");
        BrowseField tableSchema = getField(browseFields, "tableSchema");
        BrowseField sqlCommand = getField(browseFields, "sqlCommand");
        BrowseField targetTableForQuery = getField(browseFields, "targetTableForQuery");

        assertNotNull(destinationTableForLoad.getDefaultValue());
        assertNotNull(temporaryTableForLoad.getDefaultValue());
        assertNotNull(tableSchema.getDefaultValue());
        assertNotNull(sqlCommand.getDefaultValue());
        assertNotNull(targetTableForQuery.getDefaultValue());
    }

    @Test(expected = ConnectorException.class)
    public void shouldThrowConnectorExceptionWhenSchemaIsMissing() throws Exception {
        ImportableFieldsFactory factory = new ImportableFieldsFactory(_connection);

        when(_tableResource.getTable(anyString())).thenReturn(JSONUtil.newObjectNode());
        whenNew(TableResource.class).withArguments(_connection).thenReturn(_tableResource);
        factory.importableFields("test");
    }

    @Test(expected = ConnectorException.class)
    public void shouldThrowConnectorExceptionWhenTableResourceFail() throws Exception {
        ImportableFieldsFactory factory = new ImportableFieldsFactory(_connection);

        when(_tableResource.getTable(anyString())).thenReturn(JSONUtil.newObjectNode());
        whenNew(TableResource.class).withArguments(_connection).thenThrow(ConnectorException.class);
        factory.importableFields("test");
    }

    public JsonNode buildSchemaJsonNode() {
        ObjectNode node = JSONUtil.newObjectNode();
        ObjectNode schema = JSONUtil.newObjectNode();
        ObjectNode field = JSONUtil.newObjectNode();
        field.put("name", "name1");
        field.put("type", "type1");
        field.put("mode", "mode1");
        ObjectNode field2 = JSONUtil.newObjectNode();
        field2.put("name", "name2");
        field2.put("type", "type2");
        field2.put("mode", "mode2");
        ArrayNode fields = JSONUtil.getDefaultObjectMapper().createArrayNode();
        fields.add(field);
        fields.add(field2);
        schema.set("fields", fields);
        node.set("schema", schema);
        return node;
    }

    private BrowseField getField(Collection<BrowseField> browseFields, String name) {
        Iterator<BrowseField> it = browseFields.iterator();
        while (it.hasNext()) {
            BrowseField field = it.next();
            if (name.equalsIgnoreCase(field.getId())) {
                return field;
            }
        }
        return null;
    }
}