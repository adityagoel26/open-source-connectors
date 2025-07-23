//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.requests;

import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author saurav.b.sengupta
 *
 */
public class PrismRetryStrategyTest {
	
	private final CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    private final StatusLine statusLine = mock(StatusLine.class);
    private final PrismRetryStrategy retryStrategy = new PrismRetryStrategy(3);
    
    @Test
    public void testRetryWhenUnauthorized() {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_UNAUTHORIZED);
        boolean retry = retryStrategy.shouldRetry(1, response);

        assertTrue(retry);
        verify(response).getStatusLine();
        verify(statusLine).getStatusCode();
    }

    @Test
    public void testNoRetryWhenAuthorized() {
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        boolean retry = retryStrategy.shouldRetry(1, response);

        assertFalse(retry);
        verify(response).getStatusLine();
        verify(statusLine).getStatusCode();
    }

    @Test
    public void testNotRetryAfterMaxAttempts() {
        boolean retry = retryStrategy.shouldRetry(3, response);

        assertFalse(retry);
    }


}
