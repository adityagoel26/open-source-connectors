// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.send;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.jmssdk.JMSConnection;
import com.boomi.connector.jmssdk.JMSTestContext;
import com.boomi.connector.jmssdk.client.ClientTestUtils;
import com.boomi.connector.jmssdk.operations.JMSOperationConnection;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.ResponseUtil;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.util.CollectionUtil;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.TextMessage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * this test is skip for automated execution for now until adding a mechanism to remove the sent messages from the
 * destination
 */
@Ignore("Configure JMSTestContext with valid credentials to run these integration tests")
public class JMSSendOperationIT {

    private static final ObjectMapper OBJECT_MAPPER = JSONUtil.getDefaultObjectMapper();
    private static final String QUEUE_NAME = "QUEUE";
    private static final String ACTUAL_QUEUE_NAME = "integration-tests-jms";

    private final String _providerUrl;

    public JMSSendOperationIT() {
        _providerUrl = getClass().getResource("/azure-service-bus.properties").toString();
    }

    /**
     * Remove any message present in the integration test queue for performing a clean test
     */
    @Before
    public void purgeQueue() {
        JMSConnection<OperationContext> connection = createConnection(_providerUrl);
        ClientTestUtils.getLastMessage(connection, QUEUE_NAME, Message.class);
    }

    @Test
    public void sendBytesMessageToServiceBusTest() throws JMSException, IOException {
        JMSOperationConnection connection = createConnection(_providerUrl);
        JMSSendOperation sendOperation = new JMSSendOperation(connection);
        InputStream payload = createMessagePayload();
        SimpleTrackedData document = createInputDocument(QUEUE_NAME, "byte_message", payload);
        SimpleOperationResponse response = ResponseUtil.getResponse(Collections.singleton(document));

        sendOperation.executeUpdate(ResponseUtil.toRequest(Collections.singleton((ObjectData) document)), response);

        validateSendOperationResult(response.getResults(), ACTUAL_QUEUE_NAME, "BYTE_MESSAGE");
        BytesMessage message = ClientTestUtils.getLastMessage(connection, QUEUE_NAME, BytesMessage.class);
        validateSentBytesMessage(payload, message);
    }

    @Test
    public void sendMapMessageToServiceBusTest() throws JMSException, IOException {
        JMSOperationConnection connection = createConnection(_providerUrl);
        JMSSendOperation sendOperation = new JMSSendOperation(connection);
        Map<String, Object> values = CollectionUtil.<String, Object>mapBuilder().put("key1", "some value").put("random",
                UUID.randomUUID()).finishImmutable();
        InputStream payload = createMessagePayload(values);
        SimpleTrackedData document = createInputDocument(QUEUE_NAME, "map_message", payload);
        SimpleOperationResponse response = ResponseUtil.getResponse(Collections.singleton(document));

        sendOperation.executeUpdate(ResponseUtil.toRequest(Collections.singleton((ObjectData) document)), response);

        validateSendOperationResult(response.getResults(), ACTUAL_QUEUE_NAME, "MAP_MESSAGE");
        MapMessage message = ClientTestUtils.getLastMessage(connection, QUEUE_NAME, MapMessage.class);
        validateSentMapMessage(values, message);
    }

    @Test
    public void sendTextMessageToServiceBusTest() throws JMSException, IOException {
        JMSOperationConnection connection = createConnection(_providerUrl);
        JMSSendOperation sendOperation = new JMSSendOperation(connection);
        InputStream payload = createMessagePayload();
        SimpleTrackedData document = createInputDocument(QUEUE_NAME, "text_message", payload);
        SimpleOperationResponse response = ResponseUtil.getResponse(Collections.singleton(document));

        sendOperation.executeUpdate(ResponseUtil.toRequest(Collections.singleton((ObjectData) document)), response);

        validateSendOperationResult(response.getResults(), ACTUAL_QUEUE_NAME, "TEXT_MESSAGE");
        TextMessage message = ClientTestUtils.getLastMessage(connection, QUEUE_NAME, TextMessage.class);
        validateSentTextMessage(payload, message);
    }

    private static InputStream createMessagePayload() {
        return new ByteArrayInputStream(("some message" + UUID.randomUUID()).getBytes(StringUtil.UTF8_CHARSET));
    }

    private static InputStream createMessagePayload(Map<String, Object> values) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        OBJECT_MAPPER.writeValue(stream, values);
        return new ByteArrayInputStream(stream.toByteArray());
    }

    private static void validateSentBytesMessage(InputStream expectedPayload, BytesMessage actualMessage)
            throws JMSException, IOException {
        expectedPayload.reset();
        int expectedLength = expectedPayload.available();
        byte[] expectedBody = new byte[expectedLength];
        StreamUtil.readFully(expectedPayload, expectedBody);

        int actualLength = (int) actualMessage.getBodyLength();
        byte[] actualBody = new byte[actualLength];
        actualMessage.readBytes(actualBody, actualLength);

        Assert.assertEquals(expectedLength, actualLength);
        Assert.assertArrayEquals(expectedBody, actualBody);
    }

    private static void validateSentMapMessage(Map<String, Object> expectedValues, MapMessage actualMessage)
            throws JMSException {
        Assert.assertEquals(expectedValues.get("key1"), actualMessage.getObject("key1"));
        Assert.assertEquals(expectedValues.get("random").toString(), actualMessage.getObject("random"));
    }

    private static void validateSentTextMessage(InputStream expectedPayload, TextMessage actualMessage)
            throws IOException, JMSException {
        expectedPayload.reset();
        String expectedBody = StreamUtil.toString(expectedPayload, StringUtil.UTF8_CHARSET);
        String actualBody = actualMessage.getText();
        Assert.assertEquals(expectedBody, actualBody);
    }

    private static JMSOperationConnection createConnection(String providerUrl) {
        JMSTestContext context = JMSTestContext.getJMS2GenericWithAzureServiceBusContext(providerUrl);

        context.setOperationType(OperationType.CREATE);
        context.setOperationCustomType("SEND");
        context.setObjectTypeId("dynamic_destination");

        context.addOperationProperty("use_transaction", false);

        return new JMSOperationConnection(context);
    }

    private static SimpleTrackedData createInputDocument(String destination, String destinationType, Object payload) {
        MutableDynamicPropertyMap dynOpProps = new MutableDynamicPropertyMap();
        dynOpProps.addProperty("destination", destination);
        dynOpProps.addProperty("destination_type", destinationType);
        return new SimpleTrackedData(1, payload, null, null, dynOpProps);
    }

    private static void validateSendOperationResult(List<SimpleOperationResult> results, String expectedDestination,
            String expectedDestinationType) throws IOException {
        SimpleOperationResult result = CollectionUtil.getFirst(results);
        Assert.assertEquals(OperationStatus.SUCCESS, result.getStatus());
        List<byte[]> payloads = result.getPayloads();
        Assert.assertEquals(1, payloads.size());
        byte[] payloadResult = CollectionUtil.getFirst(payloads);
        JsonNode jsonNode = OBJECT_MAPPER.readTree(payloadResult);

        Assert.assertEquals(expectedDestination, jsonNode.path("destination").asText());
        Assert.assertEquals(expectedDestinationType, jsonNode.path("destinationType").asText());
        Assert.assertTrue("messageId should be present", StringUtil.isNotBlank(jsonNode.path("messageId").asText()));
    }
}
