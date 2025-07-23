// Copyright (c) 2025 Boomi, Inc.
package com.boomi.salesforce.rest.controller;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.Payload;
import com.boomi.connector.util.PayloadUtil;
import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.util.DOMUtil;
import com.boomi.util.StringUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.IOException;

public class DocumentController {

    private DocumentController() {
    }

    /**
     * Asserts that the SOQL Length did not exceed the limits for REST and Bulk Queries
     *
     * @param input ObjectData contains custom SOQL
     * @throws ConnectorException if query length exceeded the limit
     */
    public static void assertQueryLength(ObjectData input, boolean isBulkApiOperation) {
        try {
            Long dataSize = input.getDataSize();
            assertQueryLength(dataSize, isBulkApiOperation);
        } catch (IOException e) {
            throw new ConnectorException("[Error occurred when reading SOQL input] " + e.getMessage(), e);
        }
    }

    /**
     * Asserts that the SOQL Length did not exceed the limits for REST and Bulk Queries
     *
     * @param soqlLength soql length in bytes
     * @throws ConnectorException if query length exceeded the limit
     */
    public static void assertQueryLength(Long soqlLength, boolean isBulkApiOperation) {
        if (isBulkApiOperation) {
            if (soqlLength > Constants.MAX_SOQL_BULK) {
                throw new ConnectorException(
                        "SOQL length can't exceed " + Constants.MAX_SOQL_BULK + " characters. Current Length: " +
                        soqlLength);
            }
        } else {
            if (soqlLength > Constants.MAX_SOQL_REST) {
                throw new ConnectorException("SOQL length can't exceed " + Constants.MAX_SOQL_REST +
                                             " characters for REST API, use Bulk API for " + Constants.MAX_SOQL_BULK +
                                             " characters limit. Current Length: " + soqlLength);
            }
        }
    }

    /**
     * Generates XML output document in a Payload for no DELETE and UPDATE succesful operations
     *
     * @param recordId Salesforce internal record ID
     * @return Payload contains XML status output
     */
    public static Payload generateSuccessOutput(String recordId) {
        Document doc = DOMUtil.newDocument();
        Element records = doc.createElement("Result");

        Element id = doc.createElement("id");
        id.setTextContent(recordId);
        Element success = doc.createElement("success");
        success.setTextContent("true");

        records.appendChild(id);
        records.appendChild(success);
        return PayloadUtil.toPayload(records);
    }

    /**
     * Generates XML output document in a Payload for no DELETE and UPDATE failed operations
     *
     * @param recordId     Salesforce internal record ID
     * @param errorMessage failed error status message
     * @return Payload contains XML status output
     */
    public static Payload generateFailedOutput(String recordId, String errorMessage) {
        Document doc = DOMUtil.newDocument();
        Element records = doc.createElement("Result");

        Element id = doc.createElement("id");
        id.setTextContent(recordId);
        Element success = doc.createElement("success");
        success.setTextContent("false");
        Element status = doc.createElement("sf__Error");
        status.setTextContent(errorMessage);

        if (StringUtil.isNotBlank(recordId)) {
            records.appendChild(id);
        }
        records.appendChild(success);
        records.appendChild(status);
        return PayloadUtil.toPayload(records);
    }
}
