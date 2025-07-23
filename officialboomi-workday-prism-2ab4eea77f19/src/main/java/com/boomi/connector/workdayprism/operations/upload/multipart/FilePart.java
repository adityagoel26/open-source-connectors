//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.operations.upload.multipart;

import com.boomi.util.IOUtil;
import com.boomi.util.StreamUtil;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.text.MessageFormat;

/**
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class FilePart implements Part, Closeable {
    private static final String DISPOSITION = "Content-Disposition: form-data; name=\"{0}\"; filename=\"{1}\"";
    private static final String CONTENT_TYPE = "multipart/form-data; charset={1}";

    private final String filename;
    private final String fieldName;
    private final InputStream content;
    private final Charset charset;
    private final byte[] headers;

    public FilePart(InputStream content, Charset charset, String filename, String fieldName) {
        this(content, null, charset, filename, fieldName);
    }

    public FilePart(InputStream content, byte[] headers, Charset charset, String filename, String fieldName) {
        this.content = content;
        this.filename = filename;
        this.fieldName = fieldName;
        this.charset = charset;
        this.headers = headers;
    }

    @Override
    public void reset() throws IOException {
        content.reset();
    }

    @Override
    public String getDisposition() {
        return MessageFormat.format(DISPOSITION, fieldName, filename);
    }

    @Override
    public String getContentType() {
        return MessageFormat.format(CONTENT_TYPE, charset);
    }

    @Override
    public void write(OutputStream outputStream) throws IOException {
        if (headers != null) {
            outputStream.write(headers);
        }
        StreamUtil.copy(content, outputStream);
    }

    @Override
    public void close() {
        IOUtil.closeQuietly(content);
    }
}
