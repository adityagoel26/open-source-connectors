//Copyright (c) 2025 Boomi, LP.

package com.boomi.connector.workdayprism;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.util.BaseConnection;
import com.boomi.connector.workdayprism.model.AuthProvider;
import com.boomi.connector.workdayprism.model.PrismResponse;
import com.boomi.connector.workdayprism.operations.upload.multipart.FilePart;
import com.boomi.connector.workdayprism.requests.AnalyticsRequester;
import com.boomi.connector.workdayprism.requests.OpaRequester;
import com.boomi.connector.workdayprism.requests.Requester;
import com.boomi.connector.workdayprism.responses.DescribeTableResponse;
import com.boomi.connector.workdayprism.responses.ListTableResponse;
import com.boomi.connector.workdayprism.utils.Constants;
import com.boomi.util.CollectionUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.URLUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;

import org.apache.http.entity.ContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Custom implementation of BaseConnection specific for Workday Prism Connector version 2.0.
 *
 * @author juan.paccapelo <juan.paccapelo@boomi.com>
 */
public class PrismConnection<C extends BrowseContext> extends BaseConnection<C> {
    private static final Logger LOG = LogUtil.getLogger(PrismConnection.class);

    private static final int MAX_RETRIES = 2;

    private static final String OFFSET = "offset";
    private static final String LIMIT = "limit";
    private static final String COULD_NOT_RETRIEVE_THE_STORED_TABLE_SCHEMA =
            "couldn't retrieve the stored table schema";

    private final AuthProvider authProvider;

    /**
     * Creates a new PrismConnection instance
     *
     * @param context an instance of BrowseContext
     *        
     */
    public PrismConnection(C context) {
        super(context);
        authProvider = new AuthProvider(context);

    }

   

	/**
     * Returns the schema fields of the selected ObjectType from an operation cookie
     *
     * @return an instance of JsonNode with the current schema fields for the Table selected as ObjectType
     * @throws IllegalArgumentException
     *         in case it's not possible to parse the cookie and extract the schema
     */
    public JsonNode getSelectedTableSchema() {
        String cookieSchema = this.getOperationContext().getObjectDefinitionCookie(ObjectDefinitionRole.INPUT);
        try {
            return StringUtil.isBlank(cookieSchema) ? null : JSONUtil.getDefaultObjectMapper().readTree(cookieSchema);
        }
        catch (Exception e) {
            LOG.log(Level.WARNING, e.getMessage(), e);
            throw new IllegalArgumentException(COULD_NOT_RETRIEVE_THE_STORED_TABLE_SCHEMA);
        } 
    }

    /**
     * Executes a request to get a new page of Tables from the Workday API
     *
     * @param offset
     *         the offset value
     * @param limit
     *         the limit value
     * @return a {@link ListTableResponse} instance
     * @throws IOException
     *         if there's a communication error while the request is executed.
     */
    
    public ListTableResponse getTables(int offset, int limit) throws IOException {
        Map<String, String> params = CollectionUtil.<String, String>mapBuilder()
                .put(OFFSET, String.valueOf(offset))
                .put(LIMIT, String.valueOf(limit))
                .finish();
           return forGet()
                .setEndpoint(URLUtil.addQueryParameters(params.entrySet(), Constants.TABLES_ENDPOINT))
                .doRequest(ListTableResponse.class);
    }
    
   


    /**
     * Executes a request to the Describe Tables endpoint from the Workday API
     *
     * @param tableId
     *         the identifier for the table
     * @return an instance of DescribeTableResponse
     * @throws IOException
     *         if there's a communication error while the request is executed.
     */
        
    DescribeTableResponse describeTable(String tableId) throws IOException {
        return forGet()
                .setEndpoint(String.format(Constants.DESCRIBE_TABLES_ENDPOINT, tableId))
                .doRequest(DescribeTableResponse.class);
    }
    
    

    /**
     * Executes a request to the Create Table endpoint from the Workday API
     *
     * @param body an instance of ObjectData
     *         
     * @return an instance of PrismResponse
     * @throws IOException
     *         if there's a communication error while the request is executed.
     */
    public PrismResponse createTable(ObjectData body) throws IOException {
        return forPost()
                .setEndpoint(Constants.TABLES_ENDPOINT)
                .setBody(body)
                .doRequest();
    }

    /**
     * Executes a request to the Create Bucket endpoint from the Workday API
     *
     * @param body
     *         an instance of JsonNode
     * @return an instance of PrismResponse
     * @throws IOException
     *         if there's a communication error while the request is executed.
     */
    public PrismResponse createBucket(JsonNode body) throws IOException {
        return forPost()
                .setEndpoint(Constants.BUCKETS_ENDPOINT)
                .setBody(body)
                .doRequest();
    }

    /**
     * Executes a request to the Get Bucket endpoint from the Workday API
     *
     * @param bucketId
     *         a String type identifier for the bucket
     * @return an instance of PrismResponse
     * @throws IOException
     *         if there's a communication error while the request is executed.
     */
    public PrismResponse getBucket(String bucketId) throws IOException {
        return forGet().setContentType(ContentType.APPLICATION_JSON)
                .setEndpoint(URLUtil.makeUrlString(Constants.BUCKETS_ENDPOINT, bucketId))
                .doRequest();
    }

    /**
     * Executes a request to the Complete Bucket endpoint from the Workday API
     *
     * @param bucketId
     *         a String type identifier for the bucket
     * @return an instance of PrismResponse
     * @throws IOException
     *         if there's a communication error while the request is executed.
     */
    public PrismResponse completeBucket(String bucketId) throws IOException {
        return forPost().setContentType(ContentType.APPLICATION_JSON)
                .setEndpoint(String.format(Constants.COMPLETE_BUCKET_ENDPOINT, bucketId))
                .setBody(JSONUtil.newObjectNode())
                .doRequest();
    }
    
    /** Helper method to get boolean property from application context
     * @param key a String instance
     * @return boolean 
     */
    public boolean getBooleanProperty(String key) {
        return getOperationContext().getOperationProperties().getBooleanProperty(key);
    }

    /** Helper method to get long type property from application context
     * @param key a String instance
     * @param defaultValue of type long
     * @return long
     */
    public long getLongProperty(String key, long defaultValue) {
        return getOperationContext().getOperationProperties().getLongProperty(key, defaultValue);
    }

    /** Method to consume GET APIs of Workday Version 2.0 
     * @return an instance of Requester
     */
    public Requester forGet() {
        return AnalyticsRequester.get(authProvider, MAX_RETRIES);
    }

    /** Method to consume POST APIs of Workday Version 2.0 
     * @return an instance of Requester
     */
    private Requester forPost() {
        return AnalyticsRequester.post(authProvider, MAX_RETRIES).setContentType(ContentType.APPLICATION_JSON);
    }

    /**
     * Executes a request to Prism Upload Endpoint to send the given {@link InputStream} to the given Bucket ID
     *
     * @param bucketId
     *         the destination Bucket ID
     * @param filePart
     *         a {@link FilePart} instance
     * @return the Response from Prism
     * @throws IOException
     *         if an error happens executing the request
     */
    public PrismResponse upload( String bucketId, FilePart filePart) throws IOException {
        return OpaRequester.post(authProvider, MAX_RETRIES, filePart)
                .setEndpoint(String.format(Constants.UPLOAD_FILE_ENDPOINT, bucketId))
                .doRequest();
    }
}