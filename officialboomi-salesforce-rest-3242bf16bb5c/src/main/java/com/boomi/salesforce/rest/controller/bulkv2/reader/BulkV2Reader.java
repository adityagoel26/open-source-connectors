// Copyright (c) 2025 Boomi, Inc.
package com.boomi.salesforce.rest.controller.bulkv2.reader;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.Payload;
import com.boomi.connector.util.PayloadUtil;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.data.SafeInputStream;
import com.boomi.salesforce.rest.util.SalesforceResponseUtil;
import com.boomi.util.DOMUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.opencsv.CSVReader;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.InputStreamReader;

/**
 * Uses CsvReader to read and split the output to multiple payloads.<br> Limited to 1,000,000 characters per record
 */
public abstract class BulkV2Reader implements Closeable {

    private static final int BUFFER_SIZE = 8192;
    private static final int HEADER_PARTS = 2;

    protected CSVReader _csvReader;
    protected ClassicHttpResponse _response;
    protected String _jobID;
    protected int _numberRecordsProcessed;
    protected int _numberRecordsFailed;
    protected SFRestConnection _connectionManager;
    private SafeInputStream _safeInputStream;
    private String[] _headers;
    private String[] _values;

    /**
     * @param connectionManager      SFRestConnection instance
     * @param jobID                  ID of the job to be read
     * @param numberRecordsProcessed number of records Processed in the job
     * @param numberRecordsFailed    number of records failed in the job
     */
    public BulkV2Reader(SFRestConnection connectionManager, String jobID, int numberRecordsProcessed,
            int numberRecordsFailed) {
        _connectionManager = connectionManager;
        _jobID = jobID;
        _numberRecordsProcessed = numberRecordsProcessed;
        _numberRecordsFailed = numberRecordsFailed;
    }

    /**
     * Initializes the reader after either getSuccessResult, getFailedResult or getBulkQueryResult response is stored in
     * _response
     */
    protected void initReader() {
        _safeInputStream = new SafeInputStream(SalesforceResponseUtil.getContent(_response));
        _csvReader = new CSVReader(
                new BufferedReader(new InputStreamReader(_safeInputStream, StringUtil.UTF8_CHARSET), BUFFER_SIZE));
        try {
            _headers = _csvReader.readNext();
            _safeInputStream.resetMemoryCounter();
        } catch (Exception e) {
            throw new ConnectorException("[Failed to read bulk response header] " + e.getMessage(), e);
        }
    }

    /**
     * Reads and return true if could successfully read, which means getNext() method can still be called.<br> Either
     * initSuccessResultStream or initFailedResultStream methods must be called first
     *
     * @return true if next record is ready to be read
     */
    public boolean hasNext() {
        try {
            _values = _csvReader.readNext();
            return _values != null;
        } catch (Exception e) {
            throw new ConnectorException("[Failed to read CSV] " + e.getMessage(), e);
        }
    }

    /**
     * Creates and return Payload on XML Document on the current read record.<br> ReadNext method must be called first
     *
     * @return Payload contains the XML output record
     */
    public Payload getNext() {
        Document doc = DOMUtil.newDocument();
        Element root = doc.createElement(Constants.SALESFORCE_RECORDS);
        root.setAttribute("type", _connectionManager.getOperationProperties().getSObject());
        try {
            _safeInputStream.resetMemoryCounter();

            for (int i = 0; i < _headers.length; ++i) {
                String header = _headers[i];
                String value = _values[i];
                // creates an element with the header tag name and record text content
                if (StringUtil.isNotBlank(value)) {
                    Element targetParent = root;
                    while (header.contains(".")) {
                        String[] split = header.split("\\.", HEADER_PARTS);
                        NodeList list = targetParent.getElementsByTagName(split[0]);
                        if (list.getLength() == 0) {
                            Element child = doc.createElement(split[0]);
                            targetParent.appendChild(child);
                            targetParent = child;
                        } else {
                            targetParent = (Element) list.item(0);
                        }
                        header = split[1];
                    }
                    Element field = doc.createElement(header);
                    field.setTextContent(value);
                    targetParent.appendChild(field);
                }
            }
        } catch (Exception e) {
            throw new ConnectorException("[Failed to read CSV] " + e.getMessage(), e);
        }
        doc.appendChild(root);
        return PayloadUtil.toPayload(doc);
    }

    /**
     * Returns Salesforce error message the 'Sf__error' column
     */
    public String getBulkErrorMessage() {
        if (_values != null && _values.length > 1) {
            // _headers[1]: "Sf__error"
            return _values[1];
        }

        return "Salesforce failed to " + _connectionManager.getOperationProperties().getOperationBoomiName()
                + " records";
    }

    /**
     * Close the created _csvReader and _response
     */
    @Override
    public void close() {
        IOUtil.closeQuietly(_csvReader, _response);
    }
}
