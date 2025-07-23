// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.data;

import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.util.SalesforceResponseUtil;
import com.boomi.salesforce.rest.util.XMLUtils;
import com.boomi.util.IOUtil;
import com.boomi.util.TempOutputStream;

import org.apache.hc.core5.http.ClassicHttpResponse;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.stream.events.XMLEvent;

import java.io.Closeable;
import java.io.IOException;

public class CompositeResponseSplitter implements Closeable {
    private static final Integer LIMIT = 100000;

    private final ClassicHttpResponse _response;
    private final XMLStreamReader _reader;
    private TempOutputStream _output;
    private XMLStreamWriter _eventWriter;
    private String _curTagName;
    private StringBuilder _errorMessage;
    private StringBuilder _isSuccess;

    public CompositeResponseSplitter(ClassicHttpResponse response) throws XMLStreamException {
        _response = response;
        XMLInputFactory inputFact = XMLUtils.getXmlInputFactory();
        _reader = inputFact.createXMLStreamReader(SalesforceResponseUtil.getContent(response));
    }

    private boolean waitForResult() throws XMLStreamException {
        while (_reader.hasNext()) {
            if (_reader.getEventType() == XMLEvent.START_ELEMENT &&
                Constants.COMPOSITE_RESULT.equals(_reader.getLocalName())) {
                return true;
            }
            _reader.next();
        }
        return false;
    }

    public boolean hasNext() throws XMLStreamException {
        return waitForResult();
    }

    public TempOutputStream getNextResult() throws XMLStreamException {
        initOutput();
        if (!waitForResult()) {
            return null;
        }
        while (_reader.hasNext()) {
            if (_reader.getEventType() == XMLEvent.START_ELEMENT) {
                _curTagName = _reader.getLocalName();
                _eventWriter.writeStartElement(_reader.getLocalName());
            }
            if (_reader.getEventType() == XMLEvent.CHARACTERS) {
                String contentBuffer = _reader.getText();
                _eventWriter.writeCharacters(contentBuffer);

                if ("success".equals(_curTagName)) {
                    safeAppend(_isSuccess, contentBuffer);
                } else if ("message".equals(_curTagName) || "statusCode".equals(_curTagName)) {
                    safeAppend(_errorMessage, contentBuffer);
                }
            }
            if (_reader.getEventType() == XMLEvent.END_ELEMENT) {
                _eventWriter.writeEndElement();
                if (Constants.COMPOSITE_RESULT.equals(_reader.getLocalName())) {
                    break;
                }
            }
            _reader.next();
        }
        _eventWriter.close();

        return _output;
    }

    private void initOutput() throws XMLStreamException {
        IOUtil.closeQuietly(_output);
        _output = new TempOutputStream();
        XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
        _eventWriter = outputFactory.createXMLStreamWriter(_output);
        _isSuccess = new StringBuilder();
        _errorMessage = new StringBuilder();
    }

    private static void safeAppend(StringBuilder target, String value) {
        value = value.trim();
        if (value.length() != 0 && target.length() + value.length() <= LIMIT) {
            if (target.length() != 0) {
                target.append(" ");
            }
            target.append(value);
        }
    }

    public boolean wasSuccess() {
        return Boolean.parseBoolean(_isSuccess.toString());
    }

    public String getErrorMessage() {
        return _errorMessage.toString();
    }

    @Override
    public void close() throws IOException {
        _response.close();
        try {
            _reader.close();
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
        _output.close();
    }
}
