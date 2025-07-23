// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.pool;

import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.client.settings.AdapterSettings;
import com.boomi.connector.jmssdk.client.websphere.WebsphereComponentsFactory;
import com.boomi.connector.jmssdk.client.websphere.WebsphereV1Adapter;
import com.boomi.connector.jmssdk.client.websphere.WebsphereV2Adapter;
import com.boomi.connector.testutil.MutablePropertyMap;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.jms.ConnectionFactory;

import java.util.Properties;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest(WebsphereComponentsFactory.class)
public class AdapterFactoryTest {

    @Test
    public void makeWebsphereV1AdapterTest() {
        MutablePropertyMap propertyMap = new MutablePropertyMap();
        propertyMap.put("version", "V1_1");
        propertyMap.put("server_type", "WEBSPHERE_MQ_SINGLE");
        propertyMap.put("websphere_queue_manager", "queue.manager");
        propertyMap.put("websphere_channel", "the.channel");

        AdapterSettings settings = new AdapterSettings(propertyMap, new AdapterPoolSettings(propertyMap));
        AdapterFactory adapterFactory = new AdapterFactory(settings);

        mockWebsphereComponentsFactory(settings);

        try (GenericJndiBaseAdapter adapter = adapterFactory.makeObject()) {
            Assert.assertTrue("expected an instance of WebsphereV1Adapter", adapter instanceof WebsphereV1Adapter);
        }
    }

    @Test
    public void makeWebsphereV2AdapterTest() {
        MutablePropertyMap propertyMap = new MutablePropertyMap();
        propertyMap.put("version", "V2_0");
        propertyMap.put("server_type", "WEBSPHERE_MQ_SINGLE");
        propertyMap.put("websphere_queue_manager", "queue.manager");
        propertyMap.put("websphere_channel", "the.channel");

        AdapterSettings settings = new AdapterSettings(propertyMap, new AdapterPoolSettings(propertyMap));
        AdapterFactory adapterFactory = new AdapterFactory(settings);

        mockWebsphereComponentsFactory(settings);

        try (GenericJndiBaseAdapter adapter = adapterFactory.makeObject()) {
            Assert.assertTrue("expected an instance of WebsphereV2Adapter", adapter instanceof WebsphereV2Adapter);
        }
    }

    private static void mockWebsphereComponentsFactory(AdapterSettings settings) {
        mockStatic(WebsphereComponentsFactory.class);
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class, Mockito.RETURNS_DEEP_STUBS);
        when(WebsphereComponentsFactory.createConnectionFactory(settings)).thenReturn(connectionFactory);
        when(WebsphereComponentsFactory.createJMSProperties(settings)).thenReturn(new Properties());
    }
}
