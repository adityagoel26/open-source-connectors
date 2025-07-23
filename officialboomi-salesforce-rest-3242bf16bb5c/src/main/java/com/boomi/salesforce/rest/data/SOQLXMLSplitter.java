// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.data;

import com.boomi.connector.util.xml.XMLSplitter;
import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.util.SalesforceResponseUtil;
import com.boomi.util.IOUtil;
import org.apache.hc.core5.http.ClassicHttpResponse;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

/**
 * Responsible to split Salesforce Query response into multiple documents based on the "records" array element
 */
public class SOQLXMLSplitter extends XMLSplitter {
    private final ClassicHttpResponse _response;
    private String _nextPageUrl;

    public SOQLXMLSplitter(ClassicHttpResponse response) throws XMLStreamException {
        super(createInputFactory().createXMLEventReader(SalesforceResponseUtil.getContent(response)));
        _response = response;
    }

    @Override
    protected XMLEvent findNextObjectStart(boolean isFirst) throws XMLStreamException {
        StartElement event;
        do {
            event = findNextElementStart(getReader());
            if (isFirst && event != null && Constants.SALESFORCE_NEXT_PAGE_URL.equals(event.getName().getLocalPart())) {
                _nextPageUrl = getReader().getElementText();
            }
        } while (event != null && !event.getName().getLocalPart().equals(Constants.SALESFORCE_RECORDS));
        return event;
    }

    public String getNextPageUrl() {
        return _nextPageUrl;
    }

    @Override
    protected void closeReader() {
        super.closeReader();
        IOUtil.closeQuietly(_response);
    }
}
