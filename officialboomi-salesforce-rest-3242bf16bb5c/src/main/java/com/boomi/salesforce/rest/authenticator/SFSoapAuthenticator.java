// Copyright (c) 2025 Boomi, Inc.
package com.boomi.salesforce.rest.authenticator;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.Payload;
import com.boomi.connector.util.PayloadUtil;
import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.properties.ConnectionProperties;
import com.boomi.salesforce.rest.request.AuthenticationRequestExecutor;
import com.boomi.salesforce.rest.request.RequestBuilder;
import com.boomi.salesforce.rest.util.SalesforcePayloadUtil;
import com.boomi.salesforce.rest.util.XMLUtils;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.XMLUtil;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.InputStreamEntity;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import java.io.InputStream;

/**
 * Implementations of TokenManager. Responsible for authenticating via SOAP API
 */
class SFSoapAuthenticator implements TokenManager {
    private static final String SESSION_ID = "sessionId";
    private final AuthenticationRequestExecutor _requestExecutor;
    // used for SOAP authentication only
    private final RequestBuilder _requestBuilder;
    private final ConnectionProperties _connectionProperties;
    private String _sessionID;

    SFSoapAuthenticator(RequestBuilder requestBuilder, AuthenticationRequestExecutor requestExecutor,
                               ConnectionProperties connectionProperties) {
        _requestExecutor = requestExecutor;
        _requestBuilder = requestBuilder;
        _connectionProperties = connectionProperties;

        String sessionFromCache = _connectionProperties.getSessionFromCache();
        if (StringUtil.isNotBlank(sessionFromCache)) {
            _sessionID = sessionFromCache;
        } else {
            generateAccessToken();
        }
    }

    /**
     * Returns the saved sessionID
     *
     * @return sessionID string
     */
    @Override
    public String getAccessToken() {
        return _sessionID;
    }

    /**
     * Generate and return access token by getting sessionID from SOAP API and save it
     *
     * @return the new generated access token
     * @throws ConnectorException if failed to login
     */
    @Override
    public final String generateAccessToken() {
        HttpEntity body = getSOAPLoginBody();
        ClassicHttpRequest request;
        ClassicHttpResponse response = null;
        try {
            request = _requestBuilder.loginSoap(body);
            response = _requestExecutor.executeAuthenticate(request);
            _sessionID = XMLUtils.getValueSafely(response, SESSION_ID);
            _connectionProperties.cacheSession(_sessionID);
        } catch (Exception e) {
            throw new ConnectorException("[Errors occurred while executing login request] " + e.getMessage(), e);
        } finally {
            IOUtil.closeQuietly(body, response);
        }
        return _sessionID;
    }

    /**
     * Gets the login soap message from soap-login.xml file and update it with user credentials, return HttpEntity
     * contains the updated SOAP Message
     *
     * @return HttpEntity
     */
    private HttpEntity getSOAPLoginBody() {
        try {
            Document document = XMLUtil.loadSchemaFromResource(Constants.SOAP_LOGIN_FILE);

            NodeList itemsUsername = document.getElementsByTagName(Constants.SOAP_LOGIN_USERNAME);
            NodeList itemsPassword = document.getElementsByTagName(Constants.SOAP_LOGIN_PASSWORD);
            if (itemsUsername.getLength() != 0) {
                itemsUsername.item(0).setTextContent(_connectionProperties.getUsername());
            }
            if (itemsPassword.getLength() != 0) {
                itemsPassword.item(0).setTextContent(_connectionProperties.getPassword());
            }

            Payload temp = PayloadUtil.toPayload(document);
            InputStream inputData = SalesforcePayloadUtil.payloadToInputStream(temp);
            return new InputStreamEntity(inputData, ContentType.TEXT_XML);
        } catch (Exception e) {
            throw new ConnectorException("[Failed to get Login body] " + e.getMessage(), e);
        }
    }
}
