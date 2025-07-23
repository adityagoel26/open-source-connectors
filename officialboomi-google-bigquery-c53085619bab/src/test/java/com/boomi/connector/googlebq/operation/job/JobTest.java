// Copyright (c) 2019 Boomi, Inc.
package com.boomi.connector.googlebq.operation.job;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.googlebq.util.JsonResponseUtil;
import com.boomi.restlet.client.ResponseUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.restlet.data.Response;

import java.io.IOException;
import java.io.InputStream;

import static com.boomi.connector.googlebq.GoogleBqTesUtils.getResource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

/**
 * @author Rohan Jain
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ JSONUtil.class, ResponseUtil.class })
public class JobTest {

    private JsonNode _jobSuccess;
    private JsonNode _jobError;
    private InputStream _stream = mock(InputStream.class);
    private ObjectData _inputDocument = mock(ObjectData.class);
    private Response _response = mock(Response.class);
    private Job _baseJob;

    private static final String JOB_ID = "myJobId";

    @Before
    public void setup() throws IOException {
        mockStatic(JSONUtil.class);
        mockStatic(ResponseUtil.class);
        ObjectMapper mapper = new ObjectMapper();
        InputStream jobSuccess = getResource("job_success.json");
        _jobSuccess = mapper.readTree(jobSuccess);
        InputStream jobError = getResource("job_error.json");
        _jobError = mapper.readTree(jobError);
    }

    @Test
    public void testGetJobId() throws IOException {
        prepareTest(_jobSuccess);
        assertEquals(JOB_ID, _baseJob.getJobId());
    }

    @Test
    public void testGetJob() throws IOException {
        prepareTest(_jobSuccess);
        assertEquals(_jobSuccess, _baseJob.getJob());
    }

    @Test
    public void shouldReturnTrueWhenJobIsDoneAndSuccessful() throws IOException {
        prepareTest(_jobSuccess);
        assertTrue(_baseJob.isDoneAndSuccessful());
    }

    @Test
    public void shouldReturnFalseWhenJobIsNotDone() throws IOException {
        prepareTest(_jobError);
        assertFalse(_baseJob.isJobDone());
    }

    @Test
    public void shouldReturnTrueWhenJobIsSuccessful() throws IOException {
        prepareTest(_jobSuccess);
        assertTrue(_baseJob.isJobSuccessful());
    }

    @Test
    public void shouldReturnTrueWhenJobIsPending() throws IOException {
        prepareTest(_jobSuccess);
        changeJobStatus("PENDING");
        assertTrue(_baseJob.isPendingOrRunning());
    }

    @Test
    public void shouldReturnFalseWhenJobIsFailure() throws IOException {
        prepareTest(_jobError);
        assertFalse(_baseJob.isJobSuccessful());
    }

    @Test
    public void testErrorMessageResultMessage() throws IOException {
        prepareTest(_jobError);
        String message = "Error occurred when streaming data. Location : US. Reason : notFound. Message : Not found";
        assertEquals(message, _baseJob.getErrorResultMessage());
    }

    private void prepareTest(JsonNode jobNode) throws IOException {
        doReturn(_stream).when(_inputDocument).getData();
        when(JSONUtil.parseNode(_stream)).thenReturn(jobNode);
        when(ResponseUtil.validateResponse(_response)).thenReturn(true);
        when(JsonResponseUtil.extractPayload(_response)).thenReturn(jobNode);

        when(_response.isEntityAvailable()).thenReturn(true);

        _baseJob = new Job(_response);
    }

    private void changeJobStatus(String state) {
        ObjectNode nodeState = (ObjectNode) _jobSuccess.path("status");
        nodeState.put("state", state);
    }
}

