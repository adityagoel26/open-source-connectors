// Copyright (c) 2022 Boomi, Inc.
package com.boomi.connector.googlebq.util;

import com.boomi.restlet.client.ResponseUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;

import org.restlet.data.Response;

import java.io.IOException;
import java.io.InputStream;

public final class JsonResponseUtil {

    private JsonResponseUtil() {

    }

    /**
     * Extract given {@link Response} as payload to a {@link JsonNode}
     * @param response
     * @return
     * @throws IOException
     */
    public static JsonNode extractPayload(Response response) throws IOException {
        InputStream stream = null;
        JsonNode jobBody;
        try {
            stream = ResponseUtil.getStream(response);
            jobBody = JSONUtil.parseNode(stream);
        } finally {
            IOUtil.closeQuietly(stream);
        }
        return jobBody;
    }
}
