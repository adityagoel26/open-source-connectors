//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.junit.Test;

import com.boomi.connector.api.ConnectorException;

public class PrismResponseTest {


	private StatusLine statusLine = mock(StatusLine.class);
	private Logger logger = mock(Logger.class);

	@Test
	public void shouldReturnTrueWhenStatusCodeIsNotFound() {
		try (CloseableHttpResponse response = mock(CloseableHttpResponse.class);
				PrismResponse prismResponse = new PrismResponse(response)) {
			when(response.getStatusLine()).thenReturn(statusLine);
			when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
			assertTrue(prismResponse.isNotFound());
		} catch (IOException e) {
			logger.info("Error occured in Test class: " + e.getMessage());
		}
	}

	@Test
	public void shouldReturnSuccessStatus() {
		try (CloseableHttpResponse response = mock(CloseableHttpResponse.class);
				PrismResponse prismResponse = new PrismResponse(response)) {
			when(response.getStatusLine()).thenReturn(statusLine);
			when(statusLine.getStatusCode()).thenReturn(200);
			assertEquals(200, prismResponse.getStatusCode());

		} catch (IOException e) {
			logger.info("Error occured in Test class: " + e.getMessage());
		}

	}

	@Test(expected = ConnectorException.class)
	public void shouldThrowExceptionWhenStatusNotPresent() {

		try (CloseableHttpResponse response = mock(CloseableHttpResponse.class);
				PrismResponse prismResponse = new PrismResponse(response)) {
			when(response.getStatusLine()).thenReturn(null);
			prismResponse.getStatusCode();

		} catch (IOException e) {
			logger.info("Error occured in Test class: " + e.getMessage());
		} catch (ConnectorException e) {
			assertEquals("status not present", e.getMessage());
			throw e;
		}
		fail();

	}

	@Test
	public void shouldReturnTrueFor200Status() {
		try (CloseableHttpResponse response = mock(CloseableHttpResponse.class);
				PrismResponse prismResponse = new PrismResponse(response)) {
			when(response.getStatusLine()).thenReturn(statusLine);
			when(statusLine.getStatusCode()).thenReturn(200);
			assertTrue(prismResponse.isSuccess());
		} catch (IOException e) {
			logger.info("Error occured in Test class: " + e.getMessage());
		}

	}
}
