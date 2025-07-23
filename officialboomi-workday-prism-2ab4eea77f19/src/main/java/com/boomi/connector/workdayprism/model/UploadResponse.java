//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.model;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.util.IOUtil;
import java.io.Closeable;
import java.text.MessageFormat;

/**
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class UploadResponse implements Closeable {
    private static final long LINES_UPLOADED = 0L;
    private static final String UPLOAD_LINES_MESSAGE_PATTERN = "{0} - Lines uploaded: {1}";

    private final PrismResponse prismResponse;
    private final long linesUploaded;

    /**
     * Creates a new {@link UploadResponse} instance
     *
     * @param response
     *         a {@link PrismResponse} instance.
     */
    public UploadResponse(PrismResponse response) {
        this(response, LINES_UPLOADED);
    }

    /**
     * Creates a new {@link UploadResponse} instance
     *
     * @param response
     *         a {@link PrismResponse} instance.
     */
    public UploadResponse(PrismResponse response, long linesUploaded) {
        this.prismResponse = response;
        this.linesUploaded = linesUploaded;
    }

    /** Appends API response to the final response pay load of a process using ResponseUtility methods
     * @param data
     * @param opResponse
     */
    public void addResult(ObjectData data, OperationResponse opResponse) {
        String message = MessageFormat.format(UPLOAD_LINES_MESSAGE_PATTERN, prismResponse.getStatusMessage(),
                linesUploaded);
        prismResponse.addResult(data, opResponse, message);
    }
    
    /**
     * @return <code>true</code> if the internal response is a Success, <code>false</code> otherwise
     */
    public boolean isSuccess() {
        return prismResponse.isSuccess();
    }


    @Override
    public void close() {
        IOUtil.closeQuietly(prismResponse);
    }
}
