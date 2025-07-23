// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.controller.bulkv2.writer;

import com.boomi.connector.api.ConnectorException;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.controller.metadata.SObjectController;
import com.boomi.salesforce.rest.model.SObjectField;
import com.boomi.salesforce.rest.util.XMLUtils;
import com.boomi.util.IOUtil;
import com.boomi.util.MeteredOutputStream;
import com.boomi.util.StringUtil;
import com.boomi.util.TempOutputStream;
import com.opencsv.CSVWriter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Uses CsvWriter to write and combine the inputs to TempOutputStream
 */
public class BulkV2CUDWriter implements Closeable {

    private static final String CSV_WRITING_ERROR = "[Errors occurred while writing CSV data] ";

    private final SFRestConnection _connectionManager;
    /**
     * TempOutputStream to combine csv records
     */
    private TempOutputStream _outputStream;
    /**
     * wrapper meter to count bytes
     */
    private MeteredOutputStream _meteredOutputStream;
    /**
     * writer to write csv to the TempOutputStream
     */
    private CSVWriter _csvWriter;
    /**
     * contains list of fields for this operation
     */
    private List<SObjectField> _headerFieldsList;

    public BulkV2CUDWriter(SFRestConnection connectionManager) {
        _connectionManager = connectionManager;
    }

    /**
     * Initialize and prepare to be ready to receive documents and writes csv header to the TempOutputStream
     */
    public void init() {
        _outputStream = new TempOutputStream();
        _meteredOutputStream = new MeteredOutputStream(_outputStream);
        OutputStreamWriter writer = new OutputStreamWriter(_meteredOutputStream, StringUtil.UTF8_CHARSET);
        _csvWriter = new CSVWriter(writer, ',', '"', '"', "\n");
        generateHeaderFields();
        writeHeader();
    }

    private void generateHeaderFields() {
        String fieldHeader = _connectionManager.getOperationProperties().getBulkHeader();

        if (StringUtil.isNotBlank(fieldHeader)) {
            _headerFieldsList = new ArrayList<>();
            String[] slitted = fieldHeader.split(",");
            for (String field : slitted) {
                _headerFieldsList.add(new SObjectField(field, null));
            }
        } else {
            _headerFieldsList = new SObjectController(_connectionManager)
                    .buildSObject(_connectionManager.getOperationProperties().getSObject(), false).getFields();
        }
    }

    /**
     * Writes header fields names to the CSV stream
     */
    private void writeHeader() {
        try {
            String[] fields = new String[_headerFieldsList.size()];
            for (int i = 0; i < _headerFieldsList.size(); ++i) {
                fields[i] = _headerFieldsList.get(i).getName();
            }
            _csvWriter.writeNext(fields);
        } catch (Exception e) {
            throw new ConnectorException(CSV_WRITING_ERROR + e.getMessage(), e);
        }
    }

    /**
     * Accept and parses InputStream XML input then convert and write it as CSV record to the OutputStream
     */
    public void receive(InputStream record) {
        // parse and close the SizeLimited InputStream
        Document document = XMLUtils.parseQuietly(record);
        writeRow(document);
    }

    /**
     * Properly converts XML record into CSV record and write it to the OutputStream using csvWriter, which appends
     * double quotations before and after field when needed, and escapes already exists double quotations when needed
     *
     * @param record Document the sizeLimited XML input
     */
    private void writeRow(Document record) {
        XPath xpath = XPathFactory.newInstance().newXPath();
        try {
            String[] values = new String[_headerFieldsList.size()];
            for (int i = 0; i < _headerFieldsList.size(); ++i) {
                String fieldName = _headerFieldsList.get(i).getName();
                String value = "";
                String exp = "child::node()/" + fieldName.replace('.', '/');
                Element targetElement = (Element) xpath.evaluate(exp, record, XPathConstants.NODE);
                if (targetElement != null) {
                    value = targetElement.getTextContent();
                }
                values[i] = value;
            }
            _csvWriter.writeNext(values);
        } catch (Exception e) {
            throw new ConnectorException(CSV_WRITING_ERROR + e.getMessage(), e);
        }
    }

    /**
     * Converts the tempOutputStream to InputStream to be used to upload bulk batch
     */
    public InputStream getInputStream() {
        try {
            _csvWriter.flush();
            return _outputStream.toInputStream();
        } catch (IOException e) {
            throw new ConnectorException("[Failed to start bulk operation] " + e.getMessage(), e);
        }
    }

    /**
     * Close quietly the created tempOutputStream, _meteredOutputStream and _csvWriter
     */
    @Override
    public void close() {
        IOUtil.closeQuietly(_outputStream, _meteredOutputStream, _csvWriter);
    }

    /**
     * @return the content length of the CSV generated OutputStream
     */
    public long getContentLength() {
        return _meteredOutputStream.getLength();
    }

    public void receiveDelete(String id) {
        try {
            _csvWriter.writeNext(new String[]{id});
        } catch (Exception e) {
            throw new ConnectorException(CSV_WRITING_ERROR + e.getMessage(), e);
        }
    }
}
