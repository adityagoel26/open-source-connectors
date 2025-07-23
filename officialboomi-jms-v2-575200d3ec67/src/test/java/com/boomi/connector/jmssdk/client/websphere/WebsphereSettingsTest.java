// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client.websphere;

import com.boomi.connector.jmssdk.util.JMSConstants;
import com.boomi.connector.testutil.MutablePropertyMap;

import org.junit.Assert;
import org.junit.Test;

public class WebsphereSettingsTest {

    @Test
    public void equalsAndHashcodeTest() {
        MutablePropertyMap properties = new MutablePropertyMap();
        properties.put("websphere_host_name", "hostname");
        properties.put("websphere_host_port", 4242L);
        properties.put("websphere_queue_manager", "queue.manager");
        properties.put("websphere_channel", "channel");
        properties.put("websphere_host_list", "hostlist");
        properties.put("websphere_use_ssl", true);
        properties.put("websphere_ssl_suite_option", "CUSTOM_SSL_SUITE");
        properties.put("websphere_ssl_suite_text", "ssl.suite");

        MutablePropertyMap otherProperties = new MutablePropertyMap();
        otherProperties.put("websphere_host_name", "hostname2");
        otherProperties.put("websphere_host_port", 4243L);
        otherProperties.put("websphere_queue_manager", "queue.manager2");
        otherProperties.put("websphere_channel", "channel2");
        otherProperties.put("websphere_host_list", "hostlist2");
        otherProperties.put("websphere_use_ssl", true);
        otherProperties.put("websphere_ssl_suite_option", "CUSTOM_SSL_SUITE");
        otherProperties.put("websphere_ssl_suite_text", "ssl.suite2");

        JMSConstants.ServerType serverType = JMSConstants.ServerType.WEBSPHERE_MQ_SINGLE;

        WebsphereSettings settings1 = new WebsphereSettings(properties, serverType);
        WebsphereSettings settings2 = new WebsphereSettings(properties, serverType);
        WebsphereSettings settings3 = new WebsphereSettings(otherProperties, serverType);

        Assert.assertEquals(settings1, settings2);
        Assert.assertEquals(settings1.hashCode(), settings2.hashCode());

        Assert.assertNotEquals(settings1, settings3);
        Assert.assertNotEquals(settings1.hashCode(), settings3.hashCode());
    }

    @Test
    public void sslCipherSuiteTest() {
        MutablePropertyMap suiteProperties = new MutablePropertyMap();
        suiteProperties.put("websphere_ssl_suite_option", "ssl.suite");

        MutablePropertyMap suiteOverrodeProperties = new MutablePropertyMap();
        suiteOverrodeProperties.put("websphere_ssl_suite_option", "CUSTOM_SSL_SUITE");
        suiteOverrodeProperties.put("websphere_ssl_suite_text", "overridden.ssl.suite");

        WebsphereSettings settings1 = new WebsphereSettings(suiteProperties,
                JMSConstants.ServerType.WEBSPHERE_MQ_MULTI_INSTANCE);

        WebsphereSettings settings2 = new WebsphereSettings(suiteOverrodeProperties,
                JMSConstants.ServerType.WEBSPHERE_MQ_MULTI_INSTANCE);

        Assert.assertEquals("ssl.suite", settings1.getSslCipherSuite());
        Assert.assertEquals("overridden.ssl.suite", settings2.getSslCipherSuite());
    }
}
