// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.data;

import com.boomi.salesforce.rest.util.SalesforceResponseUtil;
import com.boomi.salesforce.rest.util.XMLUtils;

import org.apache.hc.core5.http.ClassicHttpResponse;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XMLValueSafeExtractor implements Closeable {
    private static final Integer LIMIT = 100000;
    private static final Integer TOTAL_LIMIT = 1000000;
    private final ClassicHttpResponse _response;
    private final XMLStreamReader _reader;
    private final List<String> _targetValues;
    private final Map<String, String> _retValues;
    private int _curTotalLength;
    private String _curTagName;

    public XMLValueSafeExtractor(ClassicHttpResponse response, List<String> targetValues) throws XMLStreamException {
        _response = response;
        XMLInputFactory inputFact = XMLUtils.getXmlInputFactory();
        _reader = inputFact.createXMLStreamReader(SalesforceResponseUtil.getContent(response));
        _targetValues = targetValues;
        _retValues = new HashMap<>();
        _curTotalLength = 0;
        initialize();
    }

    private boolean waitForValue() throws XMLStreamException {
        while (_reader.hasNext()) {
            if (_reader.getEventType() == XMLEvent.START_ELEMENT && _targetValues.contains(_reader.getLocalName())) {
                return true;
            }
            _reader.next();
        }
        return false;
    }

    private void initialize() throws XMLStreamException {
        while (waitForValue()) {
            while (_reader.hasNext()) {
                if (_reader.getEventType() == XMLEvent.START_ELEMENT) {
                    _curTagName = _reader.getLocalName();
                } else if (_reader.getEventType() == XMLEvent.CHARACTERS) {
                    safeAppend(_curTagName, _reader.getText());
                } else if (_reader.getEventType() == XMLEvent.END_ELEMENT) {
                    break;
                }
                _reader.next();
            }
        }
    }

    private void safeAppend(String key, String valueAppended) {
        valueAppended = valueAppended.trim();
        if (valueAppended.length() == 0) {
            return;
        }

        String curValue = "";
        if (_retValues.containsKey(key)) {
            curValue = _retValues.get(key);
        }
        if (curValue.length() + valueAppended.length() <= LIMIT &&
            valueAppended.length() + _curTotalLength <= TOTAL_LIMIT) {

            if (curValue.length() != 0) {
                curValue = curValue + " " + valueAppended;
            } else {
                curValue = curValue + valueAppended;
            }

            _curTotalLength += curValue.length();
            _retValues.put(key, curValue);
        }
    }

    public boolean containsKey(String key) {
        return _retValues.containsKey(key);
    }

    public String getValue(String key) {
        return _retValues.get(key);
    }

    @Override
    public void close() throws IOException {
        _response.close();
        try {
            _reader.close();
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
    }
}
