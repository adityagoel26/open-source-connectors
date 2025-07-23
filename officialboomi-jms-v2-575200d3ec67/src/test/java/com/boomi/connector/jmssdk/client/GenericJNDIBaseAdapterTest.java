// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client;

import com.boomi.connector.testutil.doubles.BytesMessageDouble;
import com.boomi.connector.testutil.doubles.MapMessageDouble;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.jms.Message;
import javax.jms.TextMessage;

import static org.hamcrest.CoreMatchers.is;

public class GenericJNDIBaseAdapterTest {

    @Test
    public void getDestinationTypeTest() {
        GenericJndiBaseAdapter adapter = Mockito.mock(GenericJndiBaseAdapter.class, Mockito.CALLS_REAL_METHODS);
        Message bytesMessage = new BytesMessageDouble();
        Message mapMessage = new MapMessageDouble();
        Message textMessage = Mockito.mock(TextMessage.class);

        DestinationType bytesDestinationType = adapter.getDestinationType(bytesMessage);
        DestinationType mapDestinationType = adapter.getDestinationType(mapMessage);
        DestinationType textDestinationType = adapter.getDestinationType(textMessage);

        Assert.assertThat(DestinationType.BYTE_MESSAGE, is(bytesDestinationType));
        Assert.assertThat(DestinationType.MAP_MESSAGE, is(mapDestinationType));
        Assert.assertThat(DestinationType.TEXT_MESSAGE, is(textDestinationType));
    }
}
