// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.controller.composite;

import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.data.CompositeResponseSplitter;
import com.boomi.util.IOUtil;

import org.apache.hc.core5.http.ClassicHttpResponse;

import javax.xml.stream.XMLStreamException;
import java.io.InputStream;

public class CompositeController {
    private final SFRestConnection _connectionManager;

    public CompositeController(SFRestConnection connectionManager) {
        _connectionManager = connectionManager;
    }

    public CompositeResponseSplitter createSObjectCollection(InputStream compositeBatch) throws XMLStreamException {
        String assignmentRuleID = _connectionManager.getOperationProperties().getAssignmentRuleId();
        ClassicHttpResponse response = _connectionManager.getRequestHandler().createCompositeCollection(compositeBatch,
                assignmentRuleID);

        boolean isSuccess = true;
        try {
            return new CompositeResponseSplitter(response);
        } catch (Exception e) {
            isSuccess = false;
            throw e;
        } finally {
            if (!isSuccess) {
                IOUtil.closeQuietly(response);
            }
        }
    }

    public CompositeResponseSplitter updateSObjectCollection(InputStream compositeBatch) throws XMLStreamException {
        String assignmentRuleID = _connectionManager.getOperationProperties().getAssignmentRuleId();
        ClassicHttpResponse createCompositeCollection = _connectionManager.getRequestHandler()
                                                                          .updateCompositeCollection(compositeBatch,
                                                                                                     assignmentRuleID);
        return new CompositeResponseSplitter(createCompositeCollection);
    }

    public CompositeResponseSplitter upsertSObjectCollection(String sobjectName, String externalIdField,
                                                             InputStream compositeBatch) throws XMLStreamException {
        String assignmentRuleID = _connectionManager.getOperationProperties().getAssignmentRuleId();
        ClassicHttpResponse createCompositeCollection = _connectionManager.getRequestHandler()
                                                                          .upsertCompositeCollection(sobjectName,
                                                                                                     externalIdField,
                                                                                                     compositeBatch,
                                                                                                     assignmentRuleID);
        return new CompositeResponseSplitter(createCompositeCollection);
    }

    public CompositeResponseSplitter deleteSObjectCollection(String deleteParameters, Boolean allOrNone)
    throws XMLStreamException {
        ClassicHttpResponse deleteCompositeCollection = _connectionManager.getRequestHandler()
                                                                          .deleteCompositeCollection(deleteParameters,
                                                                                                     allOrNone);
        return new CompositeResponseSplitter(deleteCompositeCollection);
    }
}
