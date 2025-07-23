//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.model;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.workdayprism.utils.Constants;
import com.boomi.util.ByteUnit;
import com.boomi.util.NumberUtil;
import com.boomi.util.StringUtil;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Class holding metadata related to Workday Files for Upload Operation
 *
 * @author juan.paccapelo <juan.paccapelo@boomi.com>
 */
public class UploadMetadata {
    private static final String ERROR_INVALID_NUMBER = "the field '%s' is not a valid value";
    private static final String ERROR_EMPTY_PROPERTY = "the field '%s' must be set.";
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private static final String FIELD_BUCKET_ID_UPLOAD = "bucket_id";

    private  String bucketId;
    private final String filenameSuffix;
    private final int headerLines;
    private final int maxFileSize;
    private final ObjectData data;

    /**
     * Builds a new {@link UploadMetadata} containing all the properties needed by
     * {@link com.boomi.connector.workdayprism.operations.UploadOperation}
     *
     * @param data
     *         the input object
     * @param opProps
     *         the operation properties
     * @throws ConnectorException
     *         if a required value is not given or invalid
     */
    public UploadMetadata(ObjectData data, PropertyMap opProps, String bucketIdFetched) {
        Map<String, String> docProps = data.getDynamicProperties();
        this.data = data;
        this.headerLines = getIntProperty(docProps, opProps, Constants.FIELD_HEADER_LINES);
        this.bucketId = bucketIdFetched==null?getProperty(docProps, opProps, FIELD_BUCKET_ID_UPLOAD):bucketIdFetched;
        if(bucketId==null) {
        	 this.bucketId = getProperty(docProps, opProps, Constants.FIELD_BUCKET_ID);	
        }
        this.filenameSuffix = getProperty(docProps, opProps, Constants.FIELD_FILENAME);
        this.maxFileSize = getSizeInBytes(getIntProperty(docProps, opProps, Constants.FIELD_MAX_FILE_SIZE));
    }

    /** Helper method to fetch String value from a dynamic property map based on a given key
     * @param docProps
     * @param opProps
     * @param key
     * @return String
     */
    private String getProperty(Map<String, String> docProps, PropertyMap opProps, String key) {
    	
    	String value = StringUtil.defaultIfBlank(docProps.get(key), opProps.getProperty(key));
        if (StringUtil.isBlank(value) && !key.equals(FIELD_BUCKET_ID_UPLOAD) 
        		&& !key.equals(Constants.FIELD_BUCKET_ID)) {
            throw new ConnectorException(String.format(ERROR_EMPTY_PROPERTY, key));
        }
        return value;    }

    /** Helper method to fetch integer value from a dynamic property map based on a given key
     * @param docProps
     * @param opProps
     * @param key
     * @return integer
     */
    private static int getIntProperty(Map<String, String> docProps, PropertyMap opProps, String key) {
        String docValue = docProps.get(key);
        Long opValue = opProps.getLongProperty(key);

        int parsedValue = StringUtil.isNotBlank(docValue) ? parseToInt(docValue, key) : parseToInt(opValue, key);
        if (parsedValue < 0) {
            throw new ConnectorException(String.format(ERROR_INVALID_NUMBER, key));
        }

        return parsedValue;
    }

    /** Helper method to parse a number in string format, into an integer 
     * @param value
     * @param key
     * @return integer
     */
    private static int parseToInt(String value, String key) {
        try {
            return NumberUtil.toInteger(value);
        }
        catch (NumberFormatException e) {
            throw new ConnectorException(String.format(ERROR_INVALID_NUMBER, key), e);
        }
    }
    /** Helper method to convert a number in long format, into an integer 
     * @param value
     * @param key
     * @return integer
     */
    private static int parseToInt(Long value, String key) {
        if (value == null) {
            throw new ConnectorException(String.format(ERROR_EMPTY_PROPERTY, key));
        }
        return value.intValue();
    }

    /** Helper method to determine the size of the data in terms of Bytes
     * @param megabytes
     * @return integer
     */
    private static int getSizeInBytes(int megabytes) {
        return (int) ByteUnit.byteSize(megabytes, ByteUnit.MB.name());
    }

    public int getHeaderLines() {
        return headerLines;
    }

    public String getBucketId() {
        return bucketId;
    }

    public Charset getEncoding() {
        return DEFAULT_CHARSET;
    }

    public String getFilenameSuffix() {
        return filenameSuffix;
    }

    public int getMaxFileSize() {
        return maxFileSize;
    }

    public ObjectData getData() {
        return data;
    }
}
