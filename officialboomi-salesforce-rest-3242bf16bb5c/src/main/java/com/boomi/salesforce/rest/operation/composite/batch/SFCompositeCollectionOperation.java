// Copyright (c) 2025 Boomi, Inc.
package com.boomi.salesforce.rest.operation.composite.batch;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationRequest;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.util.PayloadUtil;
import com.boomi.connector.api.TrackedData;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.controller.DocumentController;
import com.boomi.salesforce.rest.controller.composite.CompositeController;
import com.boomi.salesforce.rest.data.CompositeResponseSplitter;
import com.boomi.salesforce.rest.properties.OperationProperties;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;

import javax.xml.stream.XMLStreamException;

import java.io.InputStream;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class SFCompositeCollectionOperation {
    private static final Logger LOG = LogUtil.getLogger(SFCompositeCollectionOperation.class);

    protected final SFCompositeCombiner _combiner;
    protected final OperationRequest _request;
    protected final OperationResponse _response;
    protected final OperationProperties _operationProperties;
    protected final AtomConfig _config;
    protected final int _pageSize;
    protected final CompositeController _compositeController;

    /**
     * @param connectionManager SFRestConnection instance
     * @param request           UpdateRequest instance
     * @param response          OperationResponse instance
     * @param config            AtomConfig instance
     */
    public SFCompositeCollectionOperation(SFRestConnection connectionManager, OperationRequest request,
                                          OperationResponse response, AtomConfig config) {
        _combiner = new SFCompositeCombiner();
        _request = request;
        _response = response;
        _config = config;
        _operationProperties = connectionManager.getOperationProperties();
        _pageSize = (int) _operationProperties.getBatchCount();
        _compositeController = new CompositeController(connectionManager);
    }

    /**
     * Executes the request and return Splitter holding the response
     *
     * @param compositeBody composite request body
     * @return SobjectCollectionSplitter holding the response
     */
    protected abstract CompositeResponseSplitter executeUpdate(InputStream compositeBody) throws XMLStreamException;

    /**
     * Executes the request and return Splitter holding the response
     *
     * @param deleteParameters composite request parameters
     * @return SobjectCollectionSplitter holding the response
     */
    protected abstract CompositeResponseSplitter executeDelete(String deleteParameters, Boolean allOrNone)
    throws XMLStreamException;

    /**
     * Sends the composite request with the given batch<br> Reads and splits the response and add the successful and
     * failed results<br>
     *
     * @param compositeBatch   InputStream contains the composite batch XML body. Can be NULL for no Body
     * @param deleteParameters composite parameters for DELETE operation
     * @param batchObjects     list of input documents in this batch of bulk
     */
    public void runCompositeRequest(String deleteParameters, InputStream compositeBatch,
                                    List<? extends TrackedData> batchObjects, Boolean allOrNoneDelete) {
        CompositeResponseSplitter reader = null;
        try {
            if (compositeBatch != null) {
                reader = executeUpdate(compositeBatch);
            } else {
                reader = executeDelete(deleteParameters, allOrNoneDelete);
            }

            while (reader.hasNext()) {
                Payload p = PayloadUtil.toPayload(reader.getNextResult().toInputStream());

                if (reader.wasSuccess()) {
                    _response.addPartialResult(batchObjects, OperationStatus.SUCCESS, "200", "success", p);
                } else {
                    _response.addPartialResult(batchObjects, OperationStatus.APPLICATION_ERROR, "",
                                               reader.getErrorMessage(), p);
                }
            }

            _response.finishPartialResult(batchObjects);
        } catch (ConnectorException e) {
            LOG.log(Level.INFO, e, e::getMessage);
            _response.addCombinedResult(batchObjects, OperationStatus.APPLICATION_ERROR, e.getStatusCode(),
                    e.getMessage(), DocumentController.generateFailedOutput("", e.getMessage()));
        } catch (Exception e) {
            LOG.log(Level.INFO, e, e::getMessage);
            _response.addCombinedResult(batchObjects, OperationStatus.FAILURE, "", e.getMessage(),
                                        DocumentController.generateFailedOutput("", e.getMessage()));
        } finally {
            IOUtil.closeQuietly(reader);
        }
    }


    public void runCompositeRequest(InputStream compositeBatch, List<? extends TrackedData> inputBatch) {
        runCompositeRequest(null, compositeBatch, inputBatch, null);

    }

    public void runCompositeRequest(String parameters, List<ObjectIdData> inputBatch, Boolean allOrNone) {
        runCompositeRequest(parameters, null, inputBatch, allOrNone);
    }


}
