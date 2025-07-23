//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.responses;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Class modeling the response from Prism Token API
 *
 * @author juan.paccapelo <juan.paccapelo@boomi.com>
 */
public class TokenResponse implements Serializable {
    private static final long serialVersionUID = 20180827L;

    private String refreshToken;
    private String tokenType;
    private String accessToken;

    @JsonCreator
    public TokenResponse(@JsonProperty("access_token") String accessToken,
            @JsonProperty("refresh_token") String refreshToken, @JsonProperty("token_type") String tokenType) {
        this.refreshToken = refreshToken;
        this.tokenType = tokenType;
        this.accessToken = accessToken;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public String getTokenType() {
        return tokenType;
    }

}
