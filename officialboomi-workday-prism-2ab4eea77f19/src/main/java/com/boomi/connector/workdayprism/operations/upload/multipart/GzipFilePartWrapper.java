//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.operations.upload.multipart;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class GzipFilePartWrapper extends AbstractPartWrapper {
    private static final String CONTENT_TYPE = "application/gzip";

    public GzipFilePartWrapper(FilePart filePart) {
        super(filePart);
    }

    @Override
    public String getContentType() {
        return CONTENT_TYPE;
    }

    @Override
    public void write(OutputStream outputStream) throws IOException {
        GZIPOutputStream gzip = new GZIPOutputStream(outputStream);
        part.write(gzip);
        gzip.finish();
    }
}
