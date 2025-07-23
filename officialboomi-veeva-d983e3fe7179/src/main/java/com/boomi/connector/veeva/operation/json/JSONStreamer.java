// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.veeva.operation.json;

import org.apache.http.entity.ContentType;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * This interface is implemented to convert JSON input streams into output streams of formats accepted by Veeva.
 */
public interface JSONStreamer {

    /**
     * Converts a JSON input stream into an output stream of a format supported by the service.
     * <p>
     * The implementations, or its consumers, must explicitly close the input stream to avoid relying on jackson
     * library to do it depending on its configuration.
     *
     * @param input  the json input stream
     * @param output the output stream
     */
    void fromJsonToStream(InputStream input, OutputStream output);

    /**
     * Returns the output content type
     *
     * @return content type including MIME type
     */
    ContentType getContentType();
}
