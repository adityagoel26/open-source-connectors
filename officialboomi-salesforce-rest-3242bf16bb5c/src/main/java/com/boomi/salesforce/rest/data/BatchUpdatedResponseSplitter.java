// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.data;

import com.boomi.salesforce.rest.util.SalesforceResponseUtil;
import com.boomi.salesforce.rest.util.XMLUtils;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.TempOutputStream;

import org.apache.hc.core5.http.ClassicHttpResponse;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.stream.events.XMLEvent;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BatchUpdatedResponseSplitter {

    private static final Logger LOG = LogUtil.getLogger(BatchUpdatedResponseSplitter.class);

    private static final int LIMIT = 100_000;
    private static final String RESULT_TAG = "result";

    private final ClassicHttpResponse _response;
    private final XMLStreamReader _reader;
    private final TempOutputStream _output;
    private final XMLStreamWriter _outputWriter;
    private final StringBuilder _errorMessage;
    private final StringBuilder _patchStatus;
    private final StringBuilder _getStatus;

    private boolean _hasErrors;
    private String _curTagName;
    private boolean _isParsingResultNode;
    private boolean _hasProcessedPatchStatus;
    private boolean _hasProcessedGetStatus;

    public BatchUpdatedResponseSplitter(ClassicHttpResponse response) throws XMLStreamException {
        boolean isSuccess = false;
        try {
            _response = response;
            XMLInputFactory inputFact = XMLUtils.getXmlInputFactory();
            _reader = inputFact.createXMLStreamReader(SalesforceResponseUtil.getContent(response));

            _output = new TempOutputStream();
            XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
            _outputWriter = outputFactory.createXMLStreamWriter(_output);
            isSuccess = true;
        } finally {
            if (!isSuccess) {
                freeResources();
            }
        }

        _errorMessage = new StringBuilder();
        _patchStatus = new StringBuilder();
        _getStatus = new StringBuilder();
    }

    public BatchUpdatedResponse processResponse() throws XMLStreamException, IOException {
        try {
            while (_reader.hasNext()) {
                switch (_reader.getEventType()) {
                    case XMLEvent.START_ELEMENT:
                        processCurrentTag();
                        break;
                    case XMLEvent.CHARACTERS:
                        parseNodeContent();
                        break;
                    case XMLEvent.END_ELEMENT:
                        if (hasFinishedProcessingResult()) {
                            return buildResponse();
                        }

                        if (_isParsingResultNode) {
                            _outputWriter.writeEndElement();
                        }
                        break;
                    default:
                        // any other xml token is ignored
                        break;
                }
                _reader.next();
            }
            return buildResponse();
        } finally {
            freeResources();
        }
    }

    private boolean hasFinishedProcessingResult() {
        return _hasProcessedGetStatus && RESULT_TAG.equals(_reader.getLocalName());
    }

    private BatchUpdatedResponse buildResponse() throws IOException, XMLStreamException {
        if (_hasErrors) {
            return new BatchUpdatedResponse(_errorMessage.toString());
        } else {
            _outputWriter.flush();
            return new BatchUpdatedResponse(_output.toInputStream());
        }
    }

    private void setHasErrors(String content) {
        if (_hasErrors) {
            return;
        }
        _hasErrors = Boolean.parseBoolean(StringUtil.trim(content));
    }

    private void setStatusCode(String content) {
        // the first status code node in the response belongs to the Patch operation, the second one to the Get
        // operation
        if (!_hasProcessedPatchStatus) {
            safeAppend(_patchStatus, content);
            _hasProcessedPatchStatus = _patchStatus.length() != 0;
        } else {
            safeAppend(_getStatus, content);
            _hasProcessedGetStatus = _getStatus.length() != 0;
        }
    }

    private void setErrorMessage(String content) {
        if (!_hasProcessedGetStatus) {
            safeAppend(_errorMessage, content);
        }
    }

    private void parseNodeContent() throws XMLStreamException {
        switch (_curTagName) {
            case "hasErrors":
                setHasErrors(_reader.getText());
                break;
            case "statusCode":
                setStatusCode(_reader.getText());
                break;
            case "message":
                // fall through
            case "errorCode":
                setErrorMessage(_reader.getText());
                break;
            default:
                // ignored
        }

        if (_isParsingResultNode) {
            _outputWriter.writeCharacters(_reader.getText());
        }
    }

    private void processCurrentTag() throws XMLStreamException {
        _curTagName = _reader.getLocalName();

        if (_isParsingResultNode) {
            _outputWriter.writeStartElement(_reader.getLocalName());

            for (int i = 0; i < _reader.getAttributeCount(); ++i) {
                String attrName = _reader.getAttributeName(i).getLocalPart();
                String attrValue = _reader.getAttributeValue(i);
                _outputWriter.writeAttribute(attrName, attrValue);
            }
        }

        if (_hasProcessedGetStatus && !_hasErrors && RESULT_TAG.equals(_curTagName)) {
            _isParsingResultNode = true;
        }
    }

    private static void safeAppend(StringBuilder target, String value) {
        String sanitizedValue = value.trim();
        if (sanitizedValue.isEmpty()) {
            return;
        }

        if (target.length() + sanitizedValue.length() > LIMIT) {
            return;
        }

        if (target.length() > 0) {
            target.append(" ");
        }

        target.append(sanitizedValue);
    }

    private void freeResources() {
        closeReader();
        closeWriter();
    }

    private void closeReader() {
        try {
            if (_reader != null) {
                _reader.close();
            }
        } catch (Exception e) {
            LOG.log(Level.INFO, e, e::getMessage);
        } finally {
            IOUtil.closeQuietly(_response);
        }
    }

    private void closeWriter() {
        try {
            if (_outputWriter != null) {
                _outputWriter.close();
            }
        } catch (Exception e) {
            LOG.log(Level.INFO, e, e::getMessage);
        } finally {
            IOUtil.closeQuietly(_output);
        }
    }
}
