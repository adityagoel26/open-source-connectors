// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.connection;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.googlebq.GoogleBqConstants;
import com.boomi.connector.util.BaseConnection;

import java.io.IOException;

public class GoogleBqBaseConnection<C extends BrowseContext> extends BaseConnection<C> {
    private static final String ERROR_CANNOT_GET_OAUTH2_CONTEXT = "Cannot get OAuth token.";
    private static final String OAUTH2_OPTIONS = "oauthOptions";

    /**
     * Creates a new GoogleBqConnection instance
     *
     * @param context
     *         a {@link BrowseContext} instance.
     */
    public GoogleBqBaseConnection(C context) {
        super(context);
    }

    /**
     * Access method to retrieve the projectId from the Context
     *
     * @return a String value for the projectId property
     */
    public String getProjectId() {
        return getContext().getConnectionProperties().getProperty(GoogleBqConstants.PROP_PROJECT_ID);
    }

    /**
     * Access method to retrieve the datasetId from the Context
     *
     * @return a String value for the datasetId property
     */
    public String getDatasetId() {
        return getContext().getOperationProperties().getProperty(GoogleBqConstants.PROP_DATASET_ID);
    }
    /**
     * Getter method to access token.
     *
     * @param forceRefresh
     *         a boolean flag to indicate if a forced refresh of the access token must be done.
     * @return the access token String
     */
    public String getAccessToken(boolean forceRefresh){
        try {
            OAuth2Context oAuth2Context = getContext().getConnectionProperties().getOAuth2Context(OAUTH2_OPTIONS);
            return oAuth2Context.getOAuth2Token(forceRefresh).getAccessToken();
        }
        catch (IOException e) {
            throw new ConnectorException(ERROR_CANNOT_GET_OAUTH2_CONTEXT, e);
        }
    }


}
