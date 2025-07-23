// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.stages;

import com.boomi.connector.api.DynamicPropertyMap;

import java.io.InputStream;
import java.util.List;

public interface StageHandler {
    /**
     * Retrieves the names of all objects with a prefix of <code>prefix</code> on the cloud location
     *
     * @param prefix Prefix of the target objects
     * @return list of names of objects found
     */
    public List<String> getListObjects(String prefix);

    /**
     * Retrieves the data of the object with name <code>keyName</code> on the cloud location
     *
     * @param keyName key name of the target object
     * @return InputStream contains the object data
     */
    public InputStream download(String keyName);

    /**
     * Uploads a new object with name <code>path/uploadCount.fileFormat</code> to the cloud
     *
     * @param path path of the file
     * @param fileFormat format of the file
     * @param data    the data of the uploaded object
     * @param dataLength is the length of the data
     */
    public void upload(String path, String fileFormat, InputStream data, long dataLength);

    /**
     *  Chunk, compresses then uploads objects with name <code>path/uploadCount.fileFormat</code> to the cloud
     *
     * @param path path of the file
     * @param fileFormat format of the file
     * @param data    the data of the uploaded object
     * @param chunkSize size of each chunk
     * @param compressionActivated true if we are going to GZIP file
     * @param recordDelimiter delimiter used to delimit records
     */
    public void UploadHandler(String path, String fileFormat, InputStream data, long chunkSize, boolean compressionActivated, char recordDelimiter);

    /**
     * Uploads a file to a specified stage in Snowflake.
     *
     * @param filePath The path of the file to be uploaded.
     * @param stagePrefix The prefix or path within the Snowflake stage where the file should be uploaded.
     * @param dynamicPropertyMap A map containing dynamic properties that may be used during the upload process.
     * @return A String representing the result of the upload operation. This could be a success message,
     *         an identifier for the uploaded file, or any other relevant information.
     */
    public String upload(String filePath, String stagePrefix, DynamicPropertyMap dynamicPropertyMap);

    /**
     * Deletes an object with name <code>keyName</code> on the cloud
     *
     * @param keyName key name of the target object
     */
    public void delete(String keyName);

    /**
     * Retrieves the url of the stage location with <code>prefixPath</code> appended
     *
     * @param prefixPath the path to be appended to the stage location
     * @return url of the external stage location
     */
    public String getStageUrl(String prefixPath);

    /**
     * Retrieves the credentials to be used in Snowflake COPY INTO parameter
     *
     * @return String contains the credentials parameter to a COPY INTO statement
     */
    public String getStageCredentials();

    /**
     * Tests stage connection
     *
     */
    public void testConnection();
}
