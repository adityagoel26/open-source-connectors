// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.veeva.operation;

import org.apache.http.HttpHeaders;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Utility class to customize execute requests.
 */
public class OperationHelper {

    public static final String TEXT_CSV = "text/csv";
    public static final String APPLICATION_X_WWW_FORM_URLENCODED = "application/x-www-form-urlencoded";
    public static final String MULTIPART_FORM_DATA = "multipart/form-data";
    public static final String APPLICATION_JSON = "application/json";
    public static final String LOAD_DATA_OBJECTS_PATH = "/services/loader/load";
    public static final String EXTRACT_DATA_FILES_PATH = "/services/loader/extract";
    public static final String DELETE_OBJECT_RECORDS_PATH = "/vobjects/{object_name}";
    private static final String POST = "POST";
    private static final String PUT = "PUT";

    private OperationHelper() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Returns the content type for a given http method and api path.
     *
     * @param method the http method
     * @param path   api path
     * @return String representing the content type
     */
    public static String getContentType(String method, String path) {
        if (!POST.equals(method) && !PUT.equals(method)) {
            return APPLICATION_JSON;
        }

        switch (path) {
            case "/services/file_staging/items":
                return MULTIPART_FORM_DATA;

            case "/objects/picklists/{picklist_name}/{picklist_value_name}":
            case "/services/file_staging/items/{item}":
            case "/vobjects/{object_name}/actions/{action_name}":
            case "/vobjects/{object_name}/{object_record_id}/actions/{action_name}":
                return APPLICATION_X_WWW_FORM_URLENCODED;

            case DELETE_OBJECT_RECORDS_PATH:
            case LOAD_DATA_OBJECTS_PATH:
            case EXTRACT_DATA_FILES_PATH:
                return APPLICATION_JSON;

            case "/objects/documents/batch":
            case "/objects/documents/versions/batch":
            case "/objects/documents/renditions/batch":

            default:
                return TEXT_CSV;
        }
    }

    /**
     * Filter parameter headers to avoid including Accept from the openapi specification, which generates conflicts
     * with Veeva API, except when using the Vault Loader API which does require the Accept Header.
     *
     * @return Collection of entries with headers
     */
    static Collection<Map.Entry<String, String>> getFilteredParameterHeaders(
            Iterable<Map.Entry<String, String>> parameterHeaders, String objectTypeId) {

        Collection<Map.Entry<String, String>> headers = new ArrayList<>();
        for (Map.Entry<String, String> header : parameterHeaders) {
            if (!HttpHeaders.ACCEPT.equals(header.getKey()) || isVaultLoaderApi(objectTypeId)) {
                headers.add(header);
            }
        }
        return headers;
    }

    private static boolean isVaultLoaderApi(String objectTypeId) {
        return LOAD_DATA_OBJECTS_PATH.equals(objectTypeId) || EXTRACT_DATA_FILES_PATH.equals(objectTypeId);
    }
}
