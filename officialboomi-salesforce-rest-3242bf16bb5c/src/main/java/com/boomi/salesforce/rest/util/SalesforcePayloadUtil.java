// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.Payload;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.TempOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SalesforcePayloadUtil {

    private SalesforcePayloadUtil() {
    }

    private static final Logger LOG = LogUtil.getLogger(SalesforcePayloadUtil.class);

    /**
     * Converts payload to inputStream, if created new Stream will close the payload
     *
     * @param payload Payload to be converted
     * @return InputStream contains the content of the given Payload
     * @throws ConnectorException if failed to read
     */
    public static InputStream payloadToInputStream(Payload payload) throws ConnectorException {
        InputStream ret = null;
        try {
            ret = payload.readFrom();
        } catch (IOException e) {
            LOG.log(Level.INFO, e, e::getMessage);
        }
        if (ret == null) {
            // writes the payload to TempOutputStream
            TempOutputStream intermediateOutputStream = new TempOutputStream();
            try {
                payload.writeTo(intermediateOutputStream);
                intermediateOutputStream.flush();
                // convert TempOutputStream to InputStream and close TempOutputStream
                ret = intermediateOutputStream.toInputStream();
            } catch (IOException e) {
                throw new ConnectorException("[Errors occurred while reading data] " + e.getMessage(), e);
            } finally {
                IOUtil.closeQuietly(intermediateOutputStream, payload);
            }
        }
        return ret;
    }
}
