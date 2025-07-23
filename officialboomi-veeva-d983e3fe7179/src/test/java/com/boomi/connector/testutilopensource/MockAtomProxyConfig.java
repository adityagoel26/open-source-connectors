// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.testutilopensource;

import com.boomi.connector.api.AtomProxyConfig;

public class MockAtomProxyConfig implements AtomProxyConfig {

    @Override
    public boolean isProxyEnabled() {
        return false;
    }

    @Override
    public boolean isAuthenticationEnabled() {
        return false;
    }

    @Override
    public String getProxyHost() {
        return null;
    }

    @Override
    public String getProxyPort() {
        return null;
    }

    @Override
    public String getProxyUser() {
        return null;
    }

    @Override
    public String getProxyPassword() {
        return null;
    }

    @Override
    public String getNonProxyHostsString() {
        return null;
    }

    @Override
    public Iterable<String> getNonProxyHosts() {
        return null;
    }
}
