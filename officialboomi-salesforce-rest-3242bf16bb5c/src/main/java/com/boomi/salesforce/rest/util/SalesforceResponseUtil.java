// Copyright (c) 2025 Boomi, Inc.
package com.boomi.salesforce.rest.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.Payload;
import com.boomi.connector.util.PayloadUtil;
import com.boomi.util.LogUtil;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;

import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SalesforceResponseUtil {

    private SalesforceResponseUtil() {
    }

    private static final Logger LOG = LogUtil.getLogger(SalesforceResponseUtil.class);

    /**
     * Returns the InputStream content of the HttpResponse.<br> Returns null if no content
     */
    public static InputStream getContent(ClassicHttpResponse response) {
        try {
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                return null;
            }
            return entity.getContent();
        } catch (Exception e) {
            throw new ConnectorException("[Failed to read response] " + e.getMessage(), e);
        }
    }

    public static Payload toPayload(ClassicHttpResponse salesforceResponse) {
        if (salesforceResponse == null) {
            return null;
        }
        return PayloadUtil.toPayload(getContent(salesforceResponse));
    }

    public static String getQueryPageLocatorQuietly(ClassicHttpResponse salesforceResponse) {
        String pageLocator = null;
        if (salesforceResponse.containsHeader("Sforce-Locator")) {
            try {
                pageLocator = salesforceResponse.getHeader("Sforce-Locator").getValue();
            } catch (Exception e) {
                LOG.log(Level.INFO, e, e::getMessage);
                return null;
            }

            if ("null".equals(pageLocator)) {
                pageLocator = null;
            }
        }

        return pageLocator;
    }
}
