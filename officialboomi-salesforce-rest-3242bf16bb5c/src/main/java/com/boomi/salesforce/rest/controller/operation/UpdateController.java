// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.controller.operation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.util.XMLUtils;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import java.io.InputStream;

public class UpdateController {

    private final SFRestConnection _connection;

    private final String _recordId;
    private final String _assignmentRuleID;

    private final String _sfObjectName;
    private final Document _document;

    /**
     * @param connection   SFRestConnection instance
     * @param sfObjectName SObject Name to be updated
     * @param inputData    the InputStream containing the input document
     */
    public UpdateController(SFRestConnection connection, String sfObjectName, InputStream inputData) {
        _connection = connection;
        _sfObjectName = sfObjectName;
        _document = XMLUtils.parseQuietly(inputData);
        _recordId = removeRecordID(_document);
        _assignmentRuleID = _connection.getOperationProperties().getAssignmentRuleId();
    }

    /**
     * Remove the Record ID from the given {@link Document} and return it
     *
     * @param document to remove the Record ID
     * @return the Record ID removed from the document
     */
    private static String removeRecordID(Document document) {
        try {
            XPath xpath = XPathFactory.newInstance().newXPath();
            String exp = "child::node()/Id | child::node()/ID | child::node()/id";
            Element targetElement = (Element) xpath.evaluate(exp, document, XPathConstants.NODE);
            if (targetElement == null) {
                throw new ConnectorException("ID field is missing");
            }
            String recordId = targetElement.getTextContent();

            // Remove ID field from input document
            targetElement.getParentNode().removeChild(targetElement);

            return recordId;
        } catch (XPathExpressionException e) {
            throw new ConnectorException("[Failed to get ID field] " + e.getMessage(), e);
        }
    }

    /**
     * Gets the Id tag of the sizeLimited XML inputData, removes the Id from the input body, converts the DOM to String,
     * then sends the UPDATE request for the Id
     *
     * @throws ConnectorException if failed to UPDATE
     */
    public void updateREST() {
        _connection.getRequestHandler().executeUpdate(_sfObjectName, _recordId, _document, _assignmentRuleID);
    }

    /**
     * Composite API - Batch Resource - executes GET record request after the UPDATE request
     *
     * @return Composite Batch response
     */
    public ClassicHttpResponse updateReturnUpdatedRecord() {
        return _connection.getRequestHandler().executeUpdateReturnUpdatedRecord(_sfObjectName, _recordId, _document,
                _assignmentRuleID);
    }

    /**
     * Getter for Record ID property
     *
     * @return the Record ID
     */
    public String getRecordId() {
        return _recordId;
    }
}
