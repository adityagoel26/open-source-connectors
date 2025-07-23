//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.operations.upload;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.workdayprism.model.UploadMetadata;
import com.boomi.connector.workdayprism.operations.upload.multipart.FilePart;
import com.boomi.util.IOUtil;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator that splits its internal {@link InputStream} file in chunks by lines
 *
 * @author juan.paccapelo <juan.paccapelo@boomi.com>
 */
public class PartIterator implements Iterator<FilePart>, Closeable {
    private static final String FILENAME_PATTERN = "%s-%s.gz";
    private static final String ERROR_PROCESSING_INPUT = "error processing input data";
    private static final int EOF = -1;

    private final UploadMetadata metadata;
    private final InputStream content;
    private final byte[] headers;

    private boolean complete;
    private long lineCount;
    private long offset;
    private int partCount;

    /**
     * Creates a {@link PartIterator} to split on demand the given {@link InputStream}
     *
     * @param input
     *         file to be chunked
     * @param metadata
     *         file metadata
     * @throws ConnectorException
     *         if an error happens building the {@link PartIterator}
     */
    public PartIterator(InputStream input, UploadMetadata metadata) {
        try {
            this.content = input;
            this. metadata = metadata;
            this.partCount = 0;
            this.lineCount = metadata.getHeaderLines();
            this.headers = extractHeaderLines(input, metadata);
            this.offset = headers.length;
            this.complete = input.available() <= 0;
        }
        catch (Exception e) {
            throw new ConnectorException(e);
        }
    }

    /** Helper method to extract the first row of the CSV file and represent the same as header
     * @param input InputStream
     * @param metadata UploadMetadata
     * @return byte[]
     * @throws IOException
     */
    private static byte[] extractHeaderLines(InputStream input, UploadMetadata metadata) throws IOException {
        byte[] headers;
        try (ByteArrayOutputStream acc = new ByteArrayOutputStream()){
            int count = 0;
            int bRead;
            while ((count < metadata.getHeaderLines()) && ((bRead = input.read()) != EOF)) {
                acc.write(bRead);
                if (bRead == '\n') {
                    count++;
                }
            }
            headers = acc.toByteArray();
            return headers;
        }catch (Exception e) {
            throw new ConnectorException(e);
        }
    }

    /**
     * @return true if there are pending lines to create files, false otherwise.
     */
    @Override
    public boolean hasNext() {
        return !complete;
    }

    /**
     * @return an {@link InputStream} containing a chunk of the original file
     * @throws NoSuchElementException
     *         if there isn't any pending line to return
     */
    @Override
    public FilePart next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        try {
            String fileName = String.format(FILENAME_PATTERN, metadata.getFilenameSuffix(), partCount++);
            return new FilePart(getPartContent(), headers, metadata.getEncoding(), fileName, metadata.getBucketId());
        }
        catch (IOException e) {
            throw new ConnectorException(ERROR_PROCESSING_INPUT, e);
        }
    }

    /** Return a part file under process as an InputStream instance
     * @return InputStream
     * @throws IOException
     */
    private InputStream getPartContent() throws IOException {
        long partOffset = offset;
        int partSize = 0;
        int lineSize;
        while ((lineSize = getLineSize(content)) > 0) {
            int newAcc = partSize + lineSize + headers.length;
            // it validates if a line was already written in order to include at least one regardless the size of it
            if (newAcc > metadata.getMaxFileSize() && partSize != 0) {
                break;
            }
            partSize += lineSize;
            lineCount++;
        }
        offset += partSize;
        complete = lineSize == 0;
        content.reset();

        return new ResettableLengthLimitedInputStream(content, partSize, partOffset);
    }

    /** Helper method to get the number of rows in a file
     * @param inputStream
     * @return integer
     * @throws IOException
     */
    private static int getLineSize(InputStream inputStream) throws IOException {
        int lineSize = 0;
        for (int bRead = inputStream.read(); bRead != EOF; bRead = inputStream.read()) {
            lineSize++;
            if (bRead == '\n') {
                break;
            }
        }
        return lineSize;
    }

    /**
     * @throws UnsupportedOperationException
     *         as it's not supported
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        IOUtil.closeQuietly(content);
    }

    /**
     * @return how many lines has been processed so far
     */
    public long getLineCount() {
        return lineCount;
    }

}
