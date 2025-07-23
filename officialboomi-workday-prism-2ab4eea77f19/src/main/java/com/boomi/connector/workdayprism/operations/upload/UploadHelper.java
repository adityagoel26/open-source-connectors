//Copyright (c) 2025 Boomi, LP.

package com.boomi.connector.workdayprism.operations.upload;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.workdayprism.PrismOperationConnection;
import com.boomi.connector.workdayprism.model.PrismResponse;
import com.boomi.connector.workdayprism.model.UploadMetadata;
import com.boomi.connector.workdayprism.model.UploadResponse;
import com.boomi.connector.workdayprism.operations.upload.multipart.FilePart;
import com.boomi.connector.workdayprism.utils.Constants;
import com.boomi.util.IOUtil;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;

/**
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class UploadHelper {
    private static final String ERROR_NO_CONTENT = "there wasn't any content to upload";
    private static final String FILE_EXT = ".gz";
    private static final int MAX_ATTEMPTS = 3;
    private static final int MAX_CHUNKS_COUNT = 100;
    private static final String ERROR_MAX_CHUNK_COUNT = "The file is too big to use the Maximum chunk size defined in "
            + "the operation. The file limit and maximum for a bucket is " + MAX_CHUNKS_COUNT;

    private final PrismOperationConnection connection;

    /**
     * Creates a new UploadHelper instance
     *
     * @param connection
     *         an PrismConnection instance
     */
    public UploadHelper(PrismOperationConnection connection) {
        this.connection = connection;
    }

    /** Helper method to complete the UPLOAD operation
     * @param data
     * @param bucketId
     * @return
     * @throws IOException
     */
    public UploadResponse upload(ObjectData data, String bucketId) throws IOException {
        UploadMetadata metadata = new UploadMetadata(data, connection.getContext().getOperationProperties(), bucketId);
        InputStream content = null; 
        try {
            content = data.getData();
            long fileSize = data.getDataSize();
            return fileSize > metadata.getMaxFileSize() ? chunkedUpload(content, metadata, fileSize) :
                    simpleUpload(content, metadata);
        }
        finally {
            IOUtil.closeQuietly(content);
        }
    }

    /** Method to be executed when the file size is below 1 MB
     * @param input
     * @param metadata
     * @return UploadResponse
     * @throws IOException
     */
    private UploadResponse simpleUpload(InputStream input, UploadMetadata metadata) throws IOException {
        String filename = metadata.getFilenameSuffix() + FILE_EXT;
        FilePart filePart = new FilePart(input, metadata.getEncoding(), filename, Constants.FIELD_FILE);

        return new UploadResponse(doUpload(metadata.getBucketId(), filePart));
    }

    /** Method to be executed when the file size exceeds below 1 MB
     * @param input
     * @param metadata
     * @param size
     * @return
     * @throws IOException
     */
    private UploadResponse chunkedUpload(InputStream input, UploadMetadata metadata, long size) throws IOException {
        if ((size / metadata.getMaxFileSize()) > MAX_CHUNKS_COUNT) {
            throw new ConnectorException(ERROR_MAX_CHUNK_COUNT);
        }

        PrismResponse response = null;
        try (PartIterator iterator=new PartIterator(input, metadata)){
            if (!iterator.hasNext()) {
                throw new ConnectException(ERROR_NO_CONTENT);
            }

            long linesUploaded = 0L;
            while (iterator.hasNext()) {
                // close the response from the previous iteration
                IOUtil.closeQuietly(response);
                FilePart filePart = iterator.next();
                response = doUpload(metadata.getBucketId(), filePart);
                if (!response.isSuccess()) {
                    break;
                }
                // keep track of how many lines were effectively uploaded to the service
                linesUploaded = iterator.getLineCount();
            }

            return new UploadResponse(response, linesUploaded);
        } catch(Exception e) {
        	throw new ConnectorException(e); 
        }
        
    }

    /** Method to finally provide the upload request payload in either case of a file broken into chunks or uploaded as is
     * @param bucket
     * @param filePart
     * @return PrismResponse
     * @throws IOException
     */
    private PrismResponse doUpload(String bucket, FilePart filePart) throws IOException {
        try {
            PrismResponse response;
            int attempt = 0;
            do {
                if (attempt > 0) {
                    filePart.reset();
                }

                response = connection.upload(bucket, filePart);
                attempt++;
            } while (attempt < MAX_ATTEMPTS && !response.isSuccess());

            return response;
        }
        finally {
            IOUtil.closeQuietly(filePart);
        }
    }

}
