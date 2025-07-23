// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.veeva.operation;

import com.boomi.common.apache.http.entity.RepeatableInputStreamEntity;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.openapi.OpenAPIOperation;
import com.boomi.connector.openapi.util.OpenAPIParameter;
import com.boomi.connector.openapi.util.OpenAPIUtil;
import com.boomi.connector.veeva.VeevaOperationConnection;
import com.boomi.connector.veeva.browser.VeevaBrowser;
import com.boomi.connector.veeva.operation.json.JSONCSVStreamer;
import com.boomi.connector.veeva.operation.json.JSONFormEncodedStreamer;
import com.boomi.connector.veeva.operation.json.JSONStreamer;
import com.boomi.connector.veeva.util.ClientIDUtils;
import com.boomi.connector.veeva.util.OpenAPIAction;
import com.boomi.util.LogUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.URLUtil;
import com.boomi.util.json.JSONUtil;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

public class VeevaExecuteOperation extends OpenAPIOperation {

    private static final Logger LOG = LogUtil.getLogger(VeevaExecuteOperation.class);
    private static final String VOBJECTS_PATH = "/vobjects/";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final int UNKNOWN_LENGTH = -1;
    private static final String ENCODED_SLASH = "%2F";
    private static final String UNENCODED_SLASH = "/";

    private final String _contentType;

    public VeevaExecuteOperation(VeevaOperationConnection connection) {
        super(connection);
        OperationContext context = connection.getContext();

        if (connection.isOpenApiOperation()) {
            OpenAPIAction action = new OpenAPIAction(context.getObjectTypeId());
            _contentType = OperationHelper.getContentType(action.getMethod().name(), action.getPath());
        } else {
            _contentType = ContentType.APPLICATION_JSON.getMimeType();
        }

        logOperationExecution(context);
    }

    private static void addParametersToBody(MultipartEntityBuilder builder, Iterable<Entry<String, String>> params) {
        for (Entry<String, String> parameter : params) {
            builder.addTextBody(parameter.getKey(), parameter.getValue());
        }
    }

    private static void addQueryParameter(Collection<Entry<String, String>> params, PropertyMap props, String key) {
        if (!isQueryParameter(key)) {
            return;
        }

        String value = props.getProperty(key);
        if (StringUtil.isNotBlank(value)) {
            String paramKey = key.substring(VeevaBrowser.QUERY_PARAMETER_FIELD_ID_PREFIX.length());
            params.add(new AbstractMap.SimpleEntry<>(paramKey, value));
        }
    }

    private static boolean isQueryParameter(String key) {
        return key.startsWith(VeevaBrowser.QUERY_PARAMETER_FIELD_ID_PREFIX);
    }

    /**
     * Gets the request headers. For multipart requests, the header 'Content-Type' is not set considering it lacks the
     * boundary and throws a FileUploadException. If the operation has parameter headers, those are included in
     * {@link OperationHelper#getFilteredParameterHeaders}.
     *
     * @param data incoming request object data.
     * @return Iterable Entry List of Headers
     */
    @Override
    protected Iterable<Map.Entry<String, String>> getHeaders(ObjectData data) {
        Entry<String, String> clientIDHeader = new AbstractMap.SimpleEntry<>(ClientIDUtils.VEEVA_CLIENT_ID_HEADER,
                ClientIDUtils.buildClientID(getContext().getConnectionProperties()));

        if (isMultipartRequest()) {
            return Collections.singleton(clientIDHeader);
        }

        String path;
        if (getConnection().isOpenApiOperation()) {
            OpenAPIAction openAPIAction = new OpenAPIAction(getContext().getObjectTypeId());
            path = openAPIAction.getPath();
        } else {
            path = getContext().getObjectTypeId();
        }

        Collection<Entry<String, String>> headers = OperationHelper.getFilteredParameterHeaders(super.getHeaders(data),
                path);
        headers.add(new AbstractMap.SimpleEntry<>(CONTENT_TYPE_HEADER, _contentType));
        headers.add(clientIDHeader);
        Map<String, String> customHeaders = getConnection().getCustomHeaders(data);
        // Add only non-empty custom headers to the request
        customHeaders.entrySet().stream().filter(entry -> StringUtil.isNotEmpty(entry.getValue())).forEach(
                headers::add);

        return headers;
    }

    /**
     * Gets the Path of the URI for this operation request. For execute actions defined in the Open API specification
     * invokes its superclass implementation, for Veeva objects it joins the common path with the object name.
     *
     * @param data incoming request object data.
     * @return the Path
     */
    @Override
    protected String getPath(ObjectData data) {
        String objectTypeId = getContext().getObjectTypeId();

        String path;
        if (getConnection().isOpenApiOperation()) {
            // for OpenAPI operations, the object type ID contains the service URI with placeholders for the path params
            // invoke super#getPath to get the path param replaced by the actual values obtained from the operation
            // config
            String fullPath = getEncodedFullPath(data);
            OpenAPIAction action = new OpenAPIAction(fullPath);
            path = action.getPath();
        } else {
            path = VOBJECTS_PATH + objectTypeId;
        }
        return path;
    }

    /**
     * Gets the query parameters. For multipart requests returns an empty list, since the parameters are moved to the
     * body. For non-multipart requests of actions defined in the Open API specification invokes its superclass
     * implementation. For non-multipart requests for Veeva Objects it returns custom query parameters.
     *
     * @param data incoming request object data.
     * @return Iterable Entry List of query parameters
     */
    @Override
    protected Iterable<Entry<String, String>> getParameters(ObjectData data) {
        if (isMultipartRequest()) {
            return Collections.emptyList();
        }

        if (getConnection().isOpenApiOperation()) {
            return super.getParameters(data);
        }

        return getCustomQueryParameters();
    }

    /**
     * The SDK isn't handling the encoding of the Path Parameters, which is why this implementation is needed as a
     * workaround. We need to encode the URI to be able to send paths (file and folder names could contain characters
     * that need to be escaped.
     * An exception being the slashes, since the service expects un-encoded slashes to recognise folder hierarchy in
     * the path. (eg: folder/a%20file%20name.txt)
     * This issue is being tracked in CON-9517
     *
     * @param data to retrieve the path parameter
     * @return the encoded URI path
     */
    private String getEncodedFullPath(ObjectData data) {
        String path = getConnection().getPath();
        for (OpenAPIParameter pathParameter : getConnection().getRequestCookie().getPathParameters()) {
            // encode each path param found, but keep the slashes are those are needed un-encoded
            String value = data.getDynamicOperationProperties().getProperty(pathParameter.getId());
            value = URLUtil.urlPathEncode(value).replace(ENCODED_SLASH, UNENCODED_SLASH);
            String name = pathParameter.getName();

            path = path.replace("{" + name + "}",
                    OpenAPIUtil.getSerializedPathParameter(name, value, pathParameter.getStyle(),
                            pathParameter.isExplode(), pathParameter.getDataType()));
        }
        return path;
    }

    /**
     * For the supported content types, it returns the appropriate HttpEntity with the request data. Otherwise, it
     * invokes its superclass implementation.
     *
     * @param data incoming request object data.
     * @return the HttpEntity
     * @throws IOException if an i/o error occurs
     */
    @Override
    protected HttpEntity getEntity(ObjectData data) throws IOException {
        switch (_contentType) {
            case OperationHelper.TEXT_CSV:
                return getInputStreamEntity(new JSONCSVStreamer(), data);
            case OperationHelper.APPLICATION_X_WWW_FORM_URLENCODED:
                return getInputStreamEntity(new JSONFormEncodedStreamer(JSONUtil.getDefaultJsonFactory()), data);
            case OperationHelper.MULTIPART_FORM_DATA:
                return getMultipartEntity(data);
            default:
                return super.getEntity(data);
        }
    }

    @Override
    public VeevaOperationConnection getConnection() {
        return (VeevaOperationConnection) super.getConnection();
    }

    private InputStreamEntity getInputStreamEntity(JSONStreamer streamer, ObjectData data) throws IOException {
        OutputStream tempOutputStream = getContext().createTempOutputStream();
        streamer.fromJsonToStream(data.getData(), tempOutputStream);

        // return an entity that can be retried
        return new RepeatableInputStreamEntity(getContext().tempOutputStreamToInputStream(tempOutputStream),
                UNKNOWN_LENGTH, streamer.getContentType());
    }

    private HttpEntity getMultipartEntity(ObjectData data) {
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        addParametersToBody(builder, super.getParameters(data));
        builder.addBinaryBody("file", data.getData(), ContentType.DEFAULT_BINARY, "file");

        return builder.build();
    }

    private Iterable<Entry<String, String>> getCustomQueryParameters() {
        Collection<Entry<String, String>> queryParameters = new ArrayList<>();
        PropertyMap operationProperties = getContext().getOperationProperties();

        Set<String> keySet = operationProperties.keySet();
        for (String operationPropertyKey : keySet) {
            addQueryParameter(queryParameters, operationProperties, operationPropertyKey);
        }

        return queryParameters;
    }

    private boolean isMultipartRequest() {
        return OperationHelper.MULTIPART_FORM_DATA.equals(_contentType);
    }

    private void logOperationExecution(OperationContext context) {
        Supplier<String> messageSupplier = () -> String.format(
                "VeevaExecuteOperation: Object Type:%s, Custom Operation Type:%s, Content Type:%s, Vault URL:%s",
                context.getObjectTypeId(), context.getCustomOperationType(), _contentType, getConnection().getUrl());
        LOG.log(Level.FINE, messageSupplier);
    }
}