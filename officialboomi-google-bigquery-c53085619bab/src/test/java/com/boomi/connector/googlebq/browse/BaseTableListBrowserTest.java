package com.boomi.connector.googlebq.browse;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.connector.googlebq.resource.TableResource;
import com.boomi.util.CollectionUtil;
import com.fasterxml.jackson.databind.JsonNode;

import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.boomi.util.StringUtil.EMPTY_STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class BaseTableListBrowserTest {
    private final GoogleBqBaseConnection<BrowseContext> _connection = mock(GoogleBqBaseConnection.class);
    private final TableResource _tableResource = mock(TableResource.class);

    private final String _datasetId = "testing";
    private final ObjectDefinitions _objectDefinitions = new ObjectDefinitions();
    private final Collection<ObjectDefinitionRole> _roles = Collections.emptyList();
    private final SortedSet<String> _resources = new TreeSet<>(CollectionUtil.asImmutableSet("test1"));

    private final TestListBrowser _browser = new TestListBrowser(_connection, _tableResource);

    @Test
    public void shouldReturnEmptyObjectTypes() {
        ObjectTypes types = _browser.getObjectTypes();
        assertNotNull(types);
        assertEquals(0, types.getTypes().size());
    }

    @Test
    public void shouldReturnObjectTypes() throws Exception {
        when(_connection.getDatasetId()).thenReturn(_datasetId);
        when(_tableResource.listTables(anyString(), any(CollectionUtil.Filter.class))).thenReturn(_resources);

        ObjectTypes types = _browser.getObjectTypes();
        assertNotNull(types);
        assertEquals(1, types.getTypes().size());
        ObjectType type = types.getTypes().get(0);
        assertEquals("datasets/testing/tables/test1", type.getId());
        assertEquals("test1", type.getLabel());
    }

    @Test(expected = ConnectorException.class)
    public void shouldThrowConnectorException() throws Exception {

        when(_connection.getDatasetId()).thenReturn(_datasetId);
        doThrow(new ConnectorException(EMPTY_STRING)).when(_tableResource).listTables(anyString(),
                any(CollectionUtil.Filter.class));
        _browser.getObjectTypes();
    }

    private class TestListBrowser extends BaseTableListBrowser {

        TestListBrowser(GoogleBqBaseConnection<BrowseContext> conn, TableResource resource) {
            super(conn, resource);
        }

        @Override
        public ObjectTypes getObjectTypes() {
            return new ObjectTypes().withTypes(super.buildObjectTypes(getResourcesFilter()));
        }

        @Override
        public ObjectDefinitions getObjectDefinitions(String objectTypeId, Collection<ObjectDefinitionRole> roles) {
            return _objectDefinitions;
        }

        CollectionUtil.Filter<JsonNode> getResourcesFilter()
        {
            return new CollectionUtil.Filter<JsonNode>()
            {
                @Override
                public boolean accept( JsonNode resource )
                {
                    return true;
                }
            };
        }
    }
}
