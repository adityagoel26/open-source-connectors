// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.browse;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectionTester;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.connector.googlebq.resource.GoogleBqResource;
import com.boomi.connector.util.BaseBrowser;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;

import org.restlet.data.Response;
import org.restlet.data.Status;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation for {@link BaseBrowser}. This class is an abstract class.
 * Classes extending this class need to provide implmentation for {@link BaseBrowser#getObjectTypes()}
 * and {@link BaseBrowser#getObjectDefinitions(String, Collection)}. This class also implements
 * {@link ConnectionTester} which enables test connection for this connector.
 *
 */
public abstract class GoogleBqBrowser extends BaseBrowser implements ConnectionTester {
    private static final Logger LOG = LogUtil.getLogger(GoogleBqBrowser.class);

    private static final String SCHEMA_RESOURCE = "schema/%s.json";
    private static final String TEST_CONNECTION_ENDPOINT = "projects/%s/serviceAccount";
    private static final String NODE_ERROR = "error";
    private static final String NODE_MESSAGE = "message";

    GoogleBqBrowser(GoogleBqBaseConnection<BrowseContext> conn) {
        super(conn);
    }

    @Override
    public GoogleBqBaseConnection<BrowseContext> getConnection() {
        return (GoogleBqBaseConnection<BrowseContext>) super.getConnection();
    }

    @Override
    public void testConnection() {
        TestResource resource = new TestResource(getConnection());
        Response response = resource.getServiceAccount();
        Status status = response.getStatus();

        if (!status.isSuccess()) {
            throw new ConnectorException(getErrorMessage(response));
        }
    }

    private static String getErrorMessage(Response response) {
        Status status = response.getStatus();
        String message = status.getDescription();

        if (response.isEntityAvailable()) {
            InputStream stream = null;
            try {
                stream = response.getEntity().getStream();
                JsonNode jsonNode = JSONUtil.parseNode(stream);
                return jsonNode.path(NODE_ERROR).path(NODE_MESSAGE).asText(message);
            }
            catch (IOException e) {
                LOG.log(Level.WARNING, e.getMessage(), e);
                return message;
            }
            finally {
                IOUtil.closeQuietly(stream);
            }
        }

        return message;
    }


    static JsonNode loadResourceSchema(String resource) {
        try {
            String schemaPath = String.format(SCHEMA_RESOURCE, resource.toLowerCase());
            return JSONUtil.loadSchemaFromResource(schemaPath);
        }
        catch (IOException e) {
            throw new ConnectorException("cannot load schema profile", e);
        }
    }

    private static class TestResource extends GoogleBqResource {
        private final String _projectId;

        TestResource(GoogleBqBaseConnection<BrowseContext> connection) {
            super(connection);
            _projectId = connection.getProjectId();
        }

        /**
         * GetServiceAccount was selected as the API endpoint to test the connection because it works for both methods
         * of authentication, it does not depends on any datasource and the response provides enough information to
         * give the user a clear reason for the test failure.
         *
         * @return a new {@link Response} instance.
         */
        Response getServiceAccount() {
            try {
                return executeGet(String.format(TEST_CONNECTION_ENDPOINT, _projectId));
            }
            catch (GeneralSecurityException e) {
                throw new ConnectorException(e);
            }
        }

    }
}
