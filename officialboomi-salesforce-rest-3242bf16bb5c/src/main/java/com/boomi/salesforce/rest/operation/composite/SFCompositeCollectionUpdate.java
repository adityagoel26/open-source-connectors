// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.operation.composite;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.data.CompositeResponseSplitter;
import com.boomi.salesforce.rest.operation.composite.batch.SFCompositeUpdateOperations;

import javax.xml.stream.XMLStreamException;
import java.io.InputStream;

public class SFCompositeCollectionUpdate extends SFCompositeUpdateOperations {

    /**
     * @param connectionManager SFRestConnection instance
     * @param request           UpdateRequest instance
     * @param response          OperationResponse instance
     * @param config            AtomConfig instance
     */
    public SFCompositeCollectionUpdate(SFRestConnection connectionManager, UpdateRequest request,
                                       OperationResponse response, AtomConfig config) {
        super(connectionManager, request, response, config);
    }

    /**
     * Executes the request and return Splitter holding the response
     *
     * @param compositeBody composite request body
     * @return SobjectCollectionSplitter holding the response
     */
    @Override
    protected CompositeResponseSplitter executeUpdate(InputStream compositeBody) throws XMLStreamException {
        return _compositeController.updateSObjectCollection(compositeBody);
    }
}
