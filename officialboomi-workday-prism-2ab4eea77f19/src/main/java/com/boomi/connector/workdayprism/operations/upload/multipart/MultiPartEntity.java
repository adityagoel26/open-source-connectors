//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.operations.upload.multipart;

import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class MultiPartEntity extends BasicHttpEntity {
    private static final Charset DEFAULT_CHARSET = StandardCharsets.US_ASCII;
    private static final String CONTENT_TYPE = "multipart/form-data; boundary=";
    private static final String END_LINE = "\r\n";
    private static final String TWO_DASHES = "--";

    private final Iterable<Part> parts;
    private final String boundary;

    public MultiPartEntity(Iterable<Part> parts, String boundary) {
        super();
        this.parts = parts;
        this.boundary = boundary;
        setDefaultContentType(boundary);
    }

    private void setDefaultContentType(String boundary) {
        super.setContentType(new BasicHeader(HTTP.CONTENT_TYPE, CONTENT_TYPE + boundary));
    }

    @Override
    public void writeTo(final OutputStream out) throws IOException {

        String boundaryStart = TWO_DASHES + boundary;

        for (Part part : parts) {
            writeBytes(boundaryStart, out);
            writeEndLine(out);

            writeBytes(part.getDisposition(), out);
            writeEndLine(out);

            writeBytes(new BasicHeader(HTTP.CONTENT_TYPE, part.getContentType()).toString(), out);
            writeEndLine(out);

            writeEndLine(out);

            part.write(out);
            writeEndLine(out);
        }

        writeBytes(TWO_DASHES + boundary + TWO_DASHES, out);
    }

    private static void writeEndLine(OutputStream out) throws IOException {
        writeBytes(END_LINE, out);
    }

    private static void writeBytes(final String string, final OutputStream out) throws IOException {
        final ByteBuffer b = DEFAULT_CHARSET.encode(CharBuffer.wrap(string));
        out.write(b.array(), 0, b.limit());
    }
}
