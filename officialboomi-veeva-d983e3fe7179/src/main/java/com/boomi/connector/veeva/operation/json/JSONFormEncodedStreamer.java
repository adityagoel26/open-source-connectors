// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva.operation.json;

import com.boomi.connector.api.ConnectorException;
import com.boomi.util.IOUtil;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.apache.http.entity.ContentType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class JSONFormEncodedStreamer implements JSONStreamer {

    private final StringBuilder _body = new StringBuilder();
    private final JsonFactory _jsonFactory;

    public JSONFormEncodedStreamer(JsonFactory jsonFactory) {
        _jsonFactory = jsonFactory;
    }

    /**
     * getContext().createTempOutputStream()
     * getContext().tempOutputStreamToInputStream(tempOutputStream)
     * <p>
     * we don't allow combining documents with a single header at top?
     * nor do we allow an array
     * need to write to the outputstream
     *
     * @param input  json input stream
     * @param output form encoded input stream
     */
    @Override
    public void fromJsonToStream(InputStream input, OutputStream output) {
        JsonParser parser = null;
        try {
            parser = _jsonFactory.createParser(input);

            JsonToken element = parser.nextToken();
            //if null there is no body passed
            if (element != null) {
                if (element != JsonToken.START_OBJECT) {
                    throw new ConnectorException("JSON start object expected but got: " + element.name());
                }
                while (parser.nextToken() != null && element != JsonToken.END_OBJECT) {
                    element = parser.getCurrentToken();
                    if (JsonToken.FIELD_NAME == element) {
                        String fieldName = parser.getCurrentName();

                        if (_body.length() > 0) {
                            _body.append("&");
                        }
                        _body.append(fieldName);
                        _body.append("=");
                        parser.nextToken();
                        element = parser.getCurrentToken();
                        if (!element.name().startsWith("VALUE_")) {
                            throw new ConnectorException("JSON value expected but got " + element.name());
                        }

                        _body.append(parser.getValueAsString());
                    }
                    //skip start object
                    element = parser.getCurrentToken();
                }
                output.write(_body.toString().getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            throw new ConnectorException(e);
        } finally {
            IOUtil.closeQuietly(parser, input);
        }
    }

    /**
     * Returns the output content type for this streamer
     *
     * @return content type with MIME type form URL-encoded
     */
    @Override
    public ContentType getContentType() {
        return ContentType.APPLICATION_FORM_URLENCODED;
    }
}
