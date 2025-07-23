//Copyright (c) 2025 Boomi, LP.

package com.boomi.connector.workdayprism.requests;

import java.io.IOException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.boomi.connector.workdayprism.model.Credentials;
import com.boomi.connector.workdayprism.responses.TokenResponse;
import com.boomi.connector.workdayprism.utils.TestConstants;
import com.boomi.util.StringUtil;

/**
 * @author saurav.b.sengupta
 *
 */
public class TokenRequesterTest {

	@BeforeClass
	public static void init() {
		RequestContextHelper.setSSLContextForTest();
	}
	
	@Test
	public void testTokenRequest() throws IOException {
		Credentials credentials=new Credentials(TestConstants.API_ENDPOINT,TestConstants.CLIENT_ID, TestConstants.CLIENT_SECRET, TestConstants.REFRESH_TOKEN);
		TokenResponse tokenResponse = new TokenRequester(credentials).get();
		Assert.assertEquals("Refresh token does not match the expected value.", TestConstants.REFRESH_TOKEN, tokenResponse.getRefreshToken());
		Assert.assertEquals("Token type is incorrect. Expected 'Bearer'.", "Bearer", tokenResponse.getTokenType());
        Assert.assertFalse("Access token should not be blank.", StringUtil.isBlank(tokenResponse.getAccessToken()));
	}
}
