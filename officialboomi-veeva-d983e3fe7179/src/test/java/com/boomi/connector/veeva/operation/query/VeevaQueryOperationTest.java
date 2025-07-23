// Copyright (c) 2025 Boomi, Inc.
package com.boomi.connector.veeva.operation.query;

import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.api.QueryRequest;
import com.boomi.connector.api.Sort;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.connector.testutil.OperationResponseUtil;
import com.boomi.connector.testutil.QueryFilterBuilder;
import com.boomi.connector.testutil.QueryGroupingBuilder;
import com.boomi.connector.testutil.QuerySimpleBuilder;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.connector.testutil.SimplePayloadMetadata;
import com.boomi.connector.testutil.SimpleQueryRequest;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.connector.veeva.VeevaOperationConnection;
import com.boomi.connector.veeva.util.ExecutionUtils;
import com.boomi.util.CollectionUtil;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.TestUtil;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class VeevaQueryOperationTest {

    private static final String RESPONSE =
            "{\"responseStatus\":\"SUCCESS\",\"responseDetails\":{\"pagesize\":1000," + "\"pageoffset\":0,\n"
                    + "\"size\":2,\"total\":2},\"data\":[{\"id\":1,\"name__v\":\"Trial Master File Plan_1\"},\n"
                    + "{\"id\":2,\"name__v\":\"Trial Master File Plan_2\"}]}";

    @BeforeAll
    public static void disableLogs() {
        TestUtil.disableBoomiLog();
    }

    public static Stream<Arguments> processResponseTestCases() {
        // test case 1
        String contentWithoutNextPage =
                "{\"responseStatus\":\"SUCCESS\",\"data\":[{\"kind\":\"folder\",\"path\":\"/u9289999\","
                        + "\"name\":\"u9289999\"}]}";
        InputStream streamWithoutNextPage = new ByteArrayInputStream(
                contentWithoutNextPage.getBytes(StringUtil.UTF8_CHARSET));
        String expectedPayload1 = "{\"kind\":\"folder\",\"path\":\"/u9289999\",\"name\":\"u9289999\"}";
        String expectedNullNextPage = null;

        // test case 2
        String expectedNextPage = "https://vvtechpartner-boomi" + "-clinical01.veevavault.com/api/v23"
                + ".3/services/file_staging/items?cursor=BA7ej%2FsQGW74KQour2I4xA"
                + "%3D%3D%3A3LRC8ShsJb209DFl%2Bo8ONMKy5zNP3wLo25eyjWx8gO%2BSoF7Q"
                + "%2FGJ39dElFDSEasIMRflhtRVKlYOmkcUDPMlFn4R1kA3aF%2FYeZre"
                + "%2B28kLDJmJ1T5Hnd3QEWpZrhZ1HETUprr4HFkfJTs5ueHvx4l40iNVh%2Ffhjcb0RCBKuQMY9" + "%2FOnnXmDSHRqJJCxoQT"
                + "%2Bb6FJ&limit=1000";
        String expectedPayload2 = "{\"kind\":\"file\",\"path\":\"/lots-of-files/file-1003928605534310638\","
                + "\"name\":\"file-1003928605534310638\",\"size\":27,"
                + "\"modified_date\":\"2024-04-03T20:05:05.000Z\"}";
        String contentWithNextPage =
                "{\"responseStatus\":\"SUCCESS\",\"responseDetails\":{\"next_page\":\"" + expectedNextPage
                        + "\"},\"data\":[" + expectedPayload2 + "]}";

        InputStream streamWithNextPage = new ByteArrayInputStream(
                contentWithNextPage.getBytes(StringUtil.UTF8_CHARSET));

        return Stream.of(
                Arguments.of("Without next page URL", streamWithoutNextPage, expectedNullNextPage, expectedPayload1),
                Arguments.of("With next page URL", streamWithNextPage, expectedNextPage, expectedPayload2));
    }

    @Test
    void executeQueryTest() throws IOException {
        MutablePropertyMap opProps = new MutablePropertyMap();
        opProps.put("MAXDOCUMENTS", 28L);
        opProps.put("PAGESIZE", 2L);
        opProps.put("CUSTOM_TERMS", "select * from documents");
        QueryFilter qf = new QueryFilterBuilder(QueryGroupingBuilder.and(new QuerySimpleBuilder("id", "ne_number", "0"),
                new QuerySimpleBuilder("name__v", "LIKE", "Tr%"))).toFilter();
        Sort sort1 = new Sort();
        sort1.setProperty("id");
        sort1.setSortOrder("ASC");
        qf.withSort(sort1);

        FilterData filterData = new SimpleTrackedData(0, qf);
        QueryRequest request = new SimpleQueryRequest(filterData);
        SimpleOperationResponse response = mock(SimpleOperationResponse.class, RETURNS_DEEP_STUBS);

        VeevaOperationConnection connection = mock(VeevaOperationConnection.class);
        OperationContext context = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        when(context.getConfig()).thenReturn(null);
        when(context.getOperationProperties()).thenReturn(opProps);
        when(connection.getContext()).thenReturn(context);

        try (MockedStatic<ExecutionUtils> executionUtils = Mockito.mockStatic(ExecutionUtils.class)) {
            executionUtils.when(() -> ExecutionUtils.execute(anyString(), anyString(), any(CloseableHttpClient.class),
                    any(InputStream.class), any(VeevaOperationConnection.class))).thenReturn(httpResponse);
        }

        when(httpResponse.getEntity().getContent()).thenReturn(
                new ByteArrayInputStream(RESPONSE.getBytes(StandardCharsets.UTF_8)));
        when(httpResponse.getStatusLine().getStatusCode()).thenReturn(200);
        doNothing().when(response).addPartialResult(any(FilterData.class), any(OperationStatus.class), anyString(),
                anyString(), any(Payload.class));
        VeevaQueryOperation queryOperation = new VeevaQueryOperation(connection);
        queryOperation.executeQuery(request, response);
    }

    @Test
    void executeBuiltQueryTest() throws IOException {
        MutablePropertyMap opProps = new MutablePropertyMap();
        opProps.put("MAXDOCUMENTS", 28L);
        opProps.put("PAGESIZE", 2L);
        opProps.put("FIND", "findValue");
        opProps.put("VERSION_FILTER_OPTIONS", "LATEST_MATCHING_VERSION");
        QueryFilter qf = new QueryFilterBuilder(QueryGroupingBuilder.and(new QuerySimpleBuilder("id", "ne_number", "0"),
                new QuerySimpleBuilder("name__v", "LIKE", "Tr%"))).toFilter();
        Sort sort1 = new Sort();
        sort1.setProperty("id");
        sort1.setSortOrder("ASC");
        qf.withSort(sort1);

        List<String> selectedFields = new ArrayList<>();
        selectedFields.add("id");
        selectedFields.add("name__v");
        FilterData filterData = new SimpleTrackedData(0, qf);
        QueryRequest request = new SimpleQueryRequest(filterData);
        SimpleOperationResponse response = mock(SimpleOperationResponse.class, RETURNS_DEEP_STUBS);

        VeevaOperationConnection connection = mock(VeevaOperationConnection.class);
        OperationContext context = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        when(context.getObjectTypeId()).thenReturn("product");
        when(context.getConfig()).thenReturn(null);
        when(context.getOperationProperties()).thenReturn(opProps);
        when(context.getSelectedFields()).thenReturn(selectedFields);
        when(connection.getContext()).thenReturn(context);
        try (MockedStatic<ExecutionUtils> executionUtils = Mockito.mockStatic(ExecutionUtils.class)) {
            executionUtils.when(() -> ExecutionUtils.execute(anyString(), anyString(), any(CloseableHttpClient.class),
                    any(InputStream.class), any(VeevaOperationConnection.class))).thenReturn(httpResponse);
        }
        when(httpResponse.getEntity().getContent()).thenReturn(
                new ByteArrayInputStream(RESPONSE.getBytes(StandardCharsets.UTF_8)));
        when(httpResponse.getStatusLine().getStatusCode()).thenReturn(200);
        doNothing().when(response).addPartialResult(any(FilterData.class), any(OperationStatus.class), anyString(),
                anyString(), any(Payload.class));
        VeevaQueryOperation queryOperation = new VeevaQueryOperation(connection);
        queryOperation.executeQuery(request, response);
    }

    @Test
    void extractNextPageRequestUriTest() throws URISyntaxException {
        VeevaOperationConnection connection = mock(VeevaOperationConnection.class, RETURNS_DEEP_STUBS);
        when(connection.getContext().getObjectTypeId()).thenReturn("get_items_at_path_list");
        VeevaQueryOperation queryOperation = new VeevaQueryOperation(connection);

        String get_items_at_path_next_page_uri = queryOperation.extractNextPageRequestUri(
                "https://myvault.veevavault.com/api/v24.1/services/file_staging/items/somepath/whataboutasubfolder");
        String milestones_next_page_uri = queryOperation.extractNextPageRequestUri(
                "https://vvtechpartner-boomi-clinical01.veevavault.com/api/v20"
                        + ".2/services/file_staging/items?cursor=BeCLjr%2B%2FtSHFtexT9v%2B%2F6w%3D%3D%3AxCfvGK8qXtWf1"
                        + "%2FPSOxYXdJL1cR1cjwUvjurSz99HoB7LlJtxpMS3D6Ineyr%2FOxivBQYt1Xz6UuPdwnb7u"
                        + "%2FLxFQrKClxsxMKBcB2jOHbH0tqfpyBdWeos8ABj4KmNfUEvsKP2pJInNUh0vKrkdGjqOfqyz"
                        + "%2FQyvHxdt0SWPuRGyvG%2BUVxQu3avYq3uCk0pZZZc&limit=1000&recursive=true");

        Assertions.assertEquals(
                "/services/file_staging/items?cursor=BeCLjr%2B%2FtSHFtexT9v%2B%2F6w%3D%3D%3AxCfvGK8qXtWf1"
                        + "%2FPSOxYXdJL1cR1cjwUvjurSz99HoB7LlJtxpMS3D6Ineyr%2FOxivBQYt1Xz6UuPdwnb7u"
                        + "%2FLxFQrKClxsxMKBcB2jOHbH0tqfpyBdWeos8ABj4KmNfUEvsKP2pJInNUh0vKrkdGjqOfqyz"
                        + "%2FQyvHxdt0SWPuRGyvG%2BUVxQu3avYq3uCk0pZZZc&limit=1000&recursive=true",
                milestones_next_page_uri);

        Assertions.assertEquals("/services/file_staging/items/somepath/whataboutasubfolder",
                get_items_at_path_next_page_uri);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource({ "processResponseTestCases" })
    void processResponse(String name, InputStream responseContent, String expectedNextPage, String expectedJSONPayload)
            throws IOException {
        // arrange
        CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        when(httpResponse.getEntity().getContent()).thenReturn(responseContent);
        when(httpResponse.getStatusLine().getStatusCode()).thenReturn(200);

        Header[] headers = new Header[1];
        headers[0] = new BasicHeader("some_key", "some_value");
        when(httpResponse.getAllHeaders()).thenReturn(headers);

        QueryRequest request = mock(QueryRequest.class);
        SimpleOperationResponse response = new SimpleOperationResponse();
        SimpleTrackedData data = new SimpleTrackedData(0, null);
        OperationResponseUtil.addSimpleTrackedData(response, data);

        when(request.getFilter()).thenReturn(data);

        QueryResponseHandler responseHandler = spy(new QueryResponseHandler(request, response));
        SimplePayloadMetadata metadata = new SimplePayloadMetadata();

        // execute test

        String nextPage = VeevaQueryOperation.processResponse(httpResponse, responseHandler, metadata);
        responseHandler.finish();

        // verify

        Map<String, String> responseTrackedGroupProps = metadata.getTrackedGroups().get("response");
        Assertions.assertNotNull(responseTrackedGroupProps, "missing tracked group properties 'response'");
        Assertions.assertEquals("some_value", responseTrackedGroupProps.get("some_key"));

        Assertions.assertEquals(expectedNextPage, nextPage);
        verify(responseHandler, times(1)).addSuccess(any());

        SimpleOperationResult results = CollectionUtil.getFirst(response.getResults());
        InputStream payloadResult = new ByteArrayInputStream(CollectionUtil.getFirst(results.getPayloads()));

        String payload = StreamUtil.toString(payloadResult, StringUtil.UTF8_CHARSET);

        Assertions.assertEquals(expectedJSONPayload, payload);
    }

    @Test
    void processResponseWithApplicationError() throws IOException {
        // arrange
        CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        when(httpResponse.getEntity().getContent()).thenReturn(
                new ByteArrayInputStream("ERROR".getBytes(StringUtil.UTF8_CHARSET)));
        when(httpResponse.getStatusLine().getStatusCode()).thenReturn(500);

        QueryRequest request = mock(QueryRequest.class);
        SimpleOperationResponse response = new SimpleOperationResponse();
        SimpleTrackedData data = new SimpleTrackedData(0, null);
        OperationResponseUtil.addSimpleTrackedData(response, data);

        when(request.getFilter()).thenReturn(data);

        QueryResponseHandler responseHandler = spy(new QueryResponseHandler(request, response));

        Header[] headers = new Header[1];
        headers[0] = new BasicHeader("some_key", "some_value");
        when(httpResponse.getAllHeaders()).thenReturn(headers);
        SimplePayloadMetadata metadata = new SimplePayloadMetadata();

        // execute test

        String nextPage = VeevaQueryOperation.processResponse(httpResponse, responseHandler, metadata);
        responseHandler.finish();
        Assertions.assertEquals("[{some_key=some_value}]", metadata.getTrackedGroups().values().toString());

        // verify

        Assertions.assertNull(nextPage);
        verify(responseHandler, times(1)).addApplicationError(any(), any(), any());

        SimpleOperationResult results = CollectionUtil.getFirst(response.getResults());
        InputStream payloadResult = new ByteArrayInputStream(CollectionUtil.getFirst(results.getPayloads()));

        String payload = StreamUtil.toString(payloadResult, StringUtil.UTF8_CHARSET);

        Assertions.assertEquals("ERROR", payload);
    }

    @Test
    void buildItemsAtPathEmptyUriTest() {
        VeevaOperationConnection connection = mock(VeevaOperationConnection.class, RETURNS_DEEP_STUBS);
        VeevaQueryOperation queryOperation = new VeevaQueryOperation(connection);
        FilterData filterData = mock(FilterData.class, RETURNS_DEEP_STUBS);
        when(filterData.getDynamicOperationProperties().getProperty("PATH")).thenReturn("");
        Assertions.assertEquals("/services/file_staging/items", queryOperation.buildItemsAtPathUri(filterData));
    }

    @Test
    void buildItemsAtPathUriWithValueTest() {
        VeevaOperationConnection connection = mock(VeevaOperationConnection.class, RETURNS_DEEP_STUBS);
        VeevaQueryOperation queryOperation = new VeevaQueryOperation(connection);
        FilterData filterData = mock(FilterData.class, RETURNS_DEEP_STUBS);
        when(filterData.getDynamicOperationProperties().getProperty("PATH")).thenReturn("path/sub");
        Assertions.assertEquals("/services/file_staging/items/path/sub",
                queryOperation.buildItemsAtPathUri(filterData));
    }

    @Test
    void buildItemsAtPathUriEmptyAndRecursiveTrue() {
        VeevaOperationConnection connection = mock(VeevaOperationConnection.class, RETURNS_DEEP_STUBS);
        VeevaQueryOperation queryOperation = new VeevaQueryOperation(connection);
        FilterData filterData = mock(FilterData.class, RETURNS_DEEP_STUBS);
        when(filterData.getDynamicOperationProperties().getProperty("PATH")).thenReturn("");
        when(filterData.getDynamicOperationProperties().getProperty("RECURSIVE")).thenReturn("true");
        Assertions.assertEquals("/services/file_staging/items?recursive=true",
                queryOperation.buildItemsAtPathUri(filterData));
    }

    @Test
    void buildItemsAtPathUriWithValueAndRecursiveTrue() {
        VeevaOperationConnection connection = mock(VeevaOperationConnection.class, RETURNS_DEEP_STUBS);
        VeevaQueryOperation queryOperation = new VeevaQueryOperation(connection);
        FilterData filterData = mock(FilterData.class, RETURNS_DEEP_STUBS);
        when(filterData.getDynamicOperationProperties().getProperty("PATH")).thenReturn("path/sub");
        when(filterData.getDynamicOperationProperties().getProperty("RECURSIVE")).thenReturn("true");
        Assertions.assertEquals("/services/file_staging/items/path/sub?recursive=true",
                queryOperation.buildItemsAtPathUri(filterData));
    }
}
