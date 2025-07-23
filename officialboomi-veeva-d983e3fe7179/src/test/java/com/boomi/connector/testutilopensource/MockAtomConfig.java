// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.testutilopensource;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.api.AtomProxyConfig;

import java.util.logging.Level;

public class MockAtomConfig implements AtomConfig {

    @Override
    public String getContainerProperty(String key) {
        return null;
    }

    @Override
    public String getContainerProperty(String key, String defaultValue) {
        return null;
    }

    @Override
    public Boolean getBooleanContainerProperty(String key) {
        return false;
    }

    @Override
    public boolean getBooleanContainerProperty(String key, boolean defaultValue) {
        return false;
    }

    @Override
    public Long getLongContainerProperty(String key) {
        return Long.valueOf(0L);
    }

    @Override
    public long getLongContainerProperty(String key, long defaultValue) {
        return 0L;
    }

    @Override
    public AtomProxyConfig getProxyConfig() {
        return new MockAtomProxyConfig();
    }

    @Override
    public int getMaxPageSize() {
        return 0;
    }

    @Override
    public int getMaxNumberObjectTypes() {
        return 0;
    }

    @Override
    public int getMaxObjectTypeCookieLength() {
        return 0;
    }

    @Override
    public Level getLogLevel() {
        return null;
    }
}

