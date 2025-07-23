// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client.websphere;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.jmssdk.client.settings.AdapterSettings;
import com.boomi.connector.jmssdk.client.websphere.WebsphereComponentsFactory.DestinationFactory;
import com.boomi.connector.jmssdk.pool.AdapterPoolSettings;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.ibm.mq.jms.MQConnectionFactory;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.jms.Destination;
import javax.jms.JMSException;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class WebsphereComponentsFactoryTest {

    @Test
    public void createSingleInstanceConnectionFactoryTest() {
        final String hostName = "https://hostname.com";
        final String sslSuite = "custom.ssl.suite";

        MutablePropertyMap properties = buildCommonProperties();
        properties.put("websphere_ssl_suite_option", "CUSTOM_SSL_SUITE");
        properties.put("websphere_ssl_suite_text", sslSuite);
        properties.put("websphere_host_name", hostName);
        properties.put("websphere_host_port", 4242L);
        properties.put("version", "V1_1");
        properties.put("server_type", "WEBSPHERE_MQ_SINGLE");

        AdapterSettings settings = new AdapterSettings(properties, new AdapterPoolSettings(properties));
        MQConnectionFactory connectionFactory =
                (MQConnectionFactory) WebsphereComponentsFactory.createConnectionFactory(settings);

        assertCommonProperties(connectionFactory);
        assertEquals(hostName, connectionFactory.getHostName());
        assertEquals(sslSuite, connectionFactory.getSSLCipherSuite());
    }

    @Test
    public void createMultiInstanceConnectionFactoryTest() throws JMSException {
        final String sslSuite = "the.ssl.suite";
        final String hostList = "https://hostname1.com(4242),https://hostname2.com(4243)";

        MutablePropertyMap properties = buildCommonProperties();
        properties.put("websphere_host_list", hostList);
        properties.put("websphere_ssl_suite_option", sslSuite);
        properties.put("version", "V2_0");
        properties.put("server_type", "WEBSPHERE_MQ_MULTI_INSTANCE");

        AdapterSettings settings = new AdapterSettings(properties, new AdapterPoolSettings(properties));
        MQConnectionFactory connectionFactory =
                (MQConnectionFactory) WebsphereComponentsFactory.createConnectionFactory(settings);

        assertCommonProperties(connectionFactory);
        assertEquals(hostList, connectionFactory.getConnectionNameList());
        assertEquals(sslSuite, connectionFactory.getSSLCipherSuite());
    }

    private static MutablePropertyMap buildCommonProperties() {
        MutablePropertyMap properties = new MutablePropertyMap();
        properties.put("websphere_queue_manager", "queue.manager");
        properties.put("websphere_channel", "the.channel");
        properties.put("websphere_use_ssl", true);
        return properties;
    }

    private static void assertCommonProperties(MQConnectionFactory connectionFactory) {
        assertEquals(4242, connectionFactory.getPort());
        assertEquals("queue.manager", connectionFactory.getQueueManager());
        assertEquals("the.channel", connectionFactory.getChannel());
        assertEquals(1, connectionFactory.getTransportType());
    }

    @Test
    public void createDestinationQueueTest() throws JMSException {
        final String destinationName = "theQueue";
        final String destinationPrefix = "queue://";
        DestinationFactory queueFactoryMock = mock(DestinationFactory.class, Mockito.RETURNS_DEEP_STUBS);

        Destination queue = WebsphereComponentsFactory.createDestination(destinationPrefix + destinationName,
                queueFactoryMock, null);

        assertNotNull(queue);
        verify(queueFactoryMock, times(1)).create(destinationName);
    }

    @Test
    public void createDestinationTopicTest() throws JMSException {
        String destinationName = "theTopic";
        final String destinationPrefix = "topic://";
        DestinationFactory topicFactoryMock = mock(DestinationFactory.class, Mockito.RETURNS_DEEP_STUBS);

        Destination topic = WebsphereComponentsFactory.createDestination(destinationPrefix + destinationName, null,
                topicFactoryMock);
        assertNotNull(topic);
        verify(topicFactoryMock, times(1)).create(destinationName);
    }

    @Test
    public void createDestinationInvalidNameTest() {
        Assert.assertThrows("expected a ConnectorException", ConnectorException.class,
                () -> WebsphereComponentsFactory.createDestination("invalidDestination", null, null));
    }

    @Test
    public void createJMSPropertiesWithAuthenticationEnabledTest() {
        MutablePropertyMap propertyMap = new MutablePropertyMap();
        propertyMap.put("authentication", true);
        propertyMap.put("username", "theUsername");
        propertyMap.put("password", "thePassword");
        propertyMap.put("version", "V1_1");
        propertyMap.put("server_type", "WEBSPHERE_MQ_SINGLE");

        AdapterSettings settings = new AdapterSettings(propertyMap, new AdapterPoolSettings(propertyMap));

        Properties jmsProperties = WebsphereComponentsFactory.createJMSProperties(settings);

        assertEquals("theUsername", jmsProperties.getProperty("java.naming.security.principal"));
        assertEquals("thePassword", jmsProperties.getProperty("java.naming.security.credentials"));
    }

    @Test
    public void createJMSPropertiesWithAuthenticationDisabledTest() {
        MutablePropertyMap propertyMap = new MutablePropertyMap();
        propertyMap.put("authentication", false);
        propertyMap.put("version", "V1_1");
        propertyMap.put("server_type", "WEBSPHERE_MQ_SINGLE");

        AdapterSettings settings = new AdapterSettings(propertyMap, new AdapterPoolSettings(propertyMap));

        Properties jmsProperties = WebsphereComponentsFactory.createJMSProperties(settings);

        assertNull("expected null username", jmsProperties.getProperty("java.naming.security.principal"));
        assertNull("expected null password", jmsProperties.getProperty("java.naming.security.credentials"));
    }
}
