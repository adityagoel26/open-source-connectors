// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.authenticator;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.api.OAuth2Token;

/**
 * Implementations of TokenManager. Responsible for authenticating via OAuth2 API
 */
class SFOAuth2Authenticator implements TokenManager {

    private static final int MAX_RETRY_COUNT = 3;

    private final OAuth2Context _oAuth2Context;

    SFOAuth2Authenticator(OAuth2Context oAuth2Context) {
        _oAuth2Context = oAuth2Context;
    }

    @Override
    public String getAccessToken() {
        try {
            OAuth2Token oAuth2Token = _oAuth2Context.getOAuth2Token(false);
            return oAuth2Token.getAccessToken();
        } catch (Exception e) {
            throw new ConnectorException("[Failed to get OAuth2 Token] " + e.getMessage(), e);
        }
    }

    @Override
    public String generateAccessToken() {
        try {
            // gets the old invalid oAuth2 token
            OAuth2Token oAuth2Token = _oAuth2Context.getOAuth2Token(false);
            String oldSessionID = oAuth2Token.getAccessToken();

            int maxRetryCount = MAX_RETRY_COUNT;
            /*
             * For some reason, sometimes (mainly after long period of inactivity) it may need to be
             * done multiple times to work safely! Sometimes with a web browser, to login to
             * Salesforce via OAuth2, I need to retry multiple times!
             */
            while (maxRetryCount-- > 0 && isOldSession(oAuth2Token, oldSessionID)) {
                oAuth2Token = _oAuth2Context.getOAuth2Token(true);
            }
            return oAuth2Token.getAccessToken();
        } catch (Exception e) {
            throw new ConnectorException("[Failed to get OAuth2 Token] " + e.getMessage(), e);
        }
    }

    private static boolean isOldSession(OAuth2Token oAuth2Token, String oldSessionID) {
        return oAuth2Token.getAccessToken() == null || oAuth2Token.getAccessToken().equals(oldSessionID);
    }
}
