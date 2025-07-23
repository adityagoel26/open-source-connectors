// Copyright (c) 2022 Boomi, Inc.
package com.boomi.connector.veeva.auth.cache;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

public class SessionCacheTest {

    private SessionCache _underTest;

    @Test
    public void getCreationTimeTest() {
        //Arrange
        _underTest = new SessionCache("key", "sessionId", 10L);
        //Act
        LocalDateTime result = _underTest.getExpirationTime().truncatedTo(ChronoUnit.MINUTES);
        //Assert
        assertEquals(LocalDateTime.now().plusMinutes(10L).minusMinutes(1L).truncatedTo(ChronoUnit.MINUTES), result);
    }

    @Test
    public void getSessionIdTest() {
        //Arrange
        String sessionId = "sessionId";
        _underTest = new SessionCache("key", sessionId, 10L);
        //Act
        String result = _underTest.getSessionId();
        //Assert
        assertEquals(sessionId, result);
    }

    @Test
    public void shouldBeInvalidIfSessionExpired() {
        SessionCache sessionCache = new SessionCache("key", "session id", 0L);
        assertFalse(sessionCache.isValid());
    }

    @Test
    public void shouldBeValidUntilOneMinuteBeforeExpiration() {
        LocalDateTime dateTimeLimitBeforePadding = LocalDateTime.now().plusMinutes(9L);
        SessionCache sessionCache = new SessionCache("key", "session id", 10L);

        try (MockedStatic<LocalDateTime> mockedDateTime = mockStatic(LocalDateTime.class)) {
            mockedDateTime.when(LocalDateTime::now).thenReturn(dateTimeLimitBeforePadding);

            assertTrue(sessionCache.isValid());
        }
    }

    @Test
    public void shouldBeInvalidOneMinuteBeforeExpiration() {
        SessionCache sessionCache = new SessionCache("key", "session id", 10L);
        LocalDateTime dateTimeDuringPadding = LocalDateTime.now().plusMinutes(9L).plusSeconds(1L);

        try (MockedStatic<LocalDateTime> mockedDateTime = mockStatic(LocalDateTime.class)) {
            mockedDateTime.when(LocalDateTime::now).thenReturn(dateTimeDuringPadding);

            assertFalse(sessionCache.isValid());
        }
    }
}
