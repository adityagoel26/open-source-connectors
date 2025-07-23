// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.veeva.operation;

import com.boomi.util.CollectionUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class OperationHelperTest {

    public static Stream<Arguments> objectTypeIdSource() {
        return Stream.of(arguments("/services/loader/load", "/services/loader/extract"));
    }

    @ParameterizedTest
    @CsvSource({
            "path, GET, application/json", "path, DELETE, application/json", "path, PATCH, application/json",
            "/services/file_staging/items, POST, multipart/form-data",
            "/objects/picklists/{picklist_name}/{picklist_value_name}, POST, application/x-www-form-urlencoded",
            "/services/file_staging/items/{item}, POST, application/x-www-form-urlencoded",
            "/vobjects/{object_name}/actions/{action_name}, POST, application/x-www-form-urlencoded",
            "/vobjects/{object_name}/{object_record_id}/actions/{action_name}, POST, application/x-www-form-urlencoded",
            "/objects/documents/batch, POST, text/csv", "/objects/documents/versions/batch, POST, text/csv" })
    public void getRealContentTypeTest(String path, String httpMethod, String realContentTypeExpected) {
        //Act
        String result = OperationHelper.getContentType(httpMethod, path);

        //Assert
        assertNotNull(result);
        assertEquals(realContentTypeExpected, result);
    }

    @Test
    void getFilteredParameterHeaders() {
        Collection<Map.Entry<String, String>> headers = Collections.singletonList(
                CollectionUtil.newImmutableEntry("Content-Type", "application/json"));

        Collection<Map.Entry<String, String>> filteredHeaders = OperationHelper.getFilteredParameterHeaders(headers,
                "");

        Assertions.assertEquals(1, filteredHeaders.size());
    }

    @Test
    void getFilteredParameterHeadersWithoutAccept() {
        Collection<Map.Entry<String, String>> headers = Collections.singletonList(
                CollectionUtil.newImmutableEntry("Accept", "application/json"));

        Collection<Map.Entry<String, String>> filteredHeaders = OperationHelper.getFilteredParameterHeaders(headers,
                "");

        Assertions.assertTrue(filteredHeaders.isEmpty());
    }

    @MethodSource("objectTypeIdSource")
    @ParameterizedTest()
    void getFilteredParameterHeadersWithAccept(String objectTypeId) {
        Map.Entry<String, String> contentTypeHeader = CollectionUtil.newEntry("Content-Type", "application/json");
        Map.Entry<String, String> acceptHeader = CollectionUtil.newEntry("Accept", "application/json");

        Collection<Map.Entry<String, String>> headers = new ArrayList<>();
        headers.add(contentTypeHeader);
        headers.add(acceptHeader);

        Collection<Map.Entry<String, String>> filteredHeaders = OperationHelper.getFilteredParameterHeaders(headers,
                objectTypeId);

        Assertions.assertEquals(2, filteredHeaders.size());
        Assertions.assertTrue(filteredHeaders.contains(contentTypeHeader));
        Assertions.assertTrue(filteredHeaders.contains(acceptHeader));
    }
}
