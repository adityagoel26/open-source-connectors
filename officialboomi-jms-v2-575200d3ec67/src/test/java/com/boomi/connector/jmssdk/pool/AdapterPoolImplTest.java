// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.pool;

import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.jmssdk.JMSTestContext;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.client.settings.AdapterSettings;

import org.junit.Test;
import org.mockito.Mockito;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AdapterPoolImplTest {

    @Test
    public void returnTheSameAdapterTest() {
        JMSTestContext context =
                new JMSTestContext.Builder().withPoolEnabled().withVersion2().withGenericService().build();
        PropertyMap connectionProperties = context.getConnectionProperties();
        AdapterSettings settings = new AdapterSettings(connectionProperties,
                new AdapterPoolSettings(connectionProperties));

        GenericJndiBaseAdapter adapterMock = mock(GenericJndiBaseAdapter.class);

        AdapterFactory factoryMock = mock(AdapterFactory.class);
        when(factoryMock.makeObject()).thenReturn(adapterMock);
        when(factoryMock.validateObject(adapterMock)).thenReturn(true);

        AdapterPool adapterPool = new AdapterPoolImpl(settings, factoryMock);

        GenericJndiBaseAdapter adapter1 = adapterPool.createAdapter();
        adapterPool.releaseAdapter(adapter1);
        GenericJndiBaseAdapter adapter2 = adapterPool.createAdapter();
        adapterPool.releaseAdapter(adapter2);

        assertThat(adapter1, is(adapter2));

        // only one adapter should have been created
        verify(factoryMock, Mockito.times(1)).makeObject();

        // the adapters are validated twice (before returning it and after being release)
        // this adapter should have been validated four times: the first after being created (true), the second after
        // being released (true), the third before being returned again (true), and the last one when realised again
        // (true).
        verify(factoryMock, Mockito.times(4)).validateObject(adapterMock);
    }

    @Test
    public void createAnotherAdapterIfValidationFailsTest() {
        JMSTestContext context =
                new JMSTestContext.Builder().withPoolEnabled().withVersion2().withGenericService().build();
        PropertyMap connectionProperties = context.getConnectionProperties();
        AdapterSettings settings = new AdapterSettings(connectionProperties,
                new AdapterPoolSettings(connectionProperties));

        GenericJndiBaseAdapter adapterMock1 = mock(GenericJndiBaseAdapter.class);
        GenericJndiBaseAdapter adapterMock2 = mock(GenericJndiBaseAdapter.class);

        AdapterFactory factoryMock = mock(AdapterFactory.class);
        when(factoryMock.makeObject()).thenReturn(adapterMock1, adapterMock2);
        // the adapter validation success the first and second time, but fails the third time before being returned
        // again
        when(factoryMock.validateObject(adapterMock1)).thenReturn(true, true, false);
        when(factoryMock.validateObject(adapterMock2)).thenReturn(true);

        AdapterPool adapterPool = new AdapterPoolImpl(settings, factoryMock);

        GenericJndiBaseAdapter adapter = adapterPool.createAdapter();
        adapterPool.releaseAdapter(adapter);
        adapter = adapterPool.createAdapter();
        adapterPool.releaseAdapter(adapter);

        // two adapters should have been created
        verify(factoryMock, Mockito.times(2)).makeObject();

        // the adapters are validated twice (before returning it and after being release)
        // this adapter should have been validated thrice: the first after being created (true), the second after being
        // released (true) and a last time before being returned (false).
        verify(factoryMock, Mockito.times(3)).validateObject(adapterMock1);
        // this adapter should have been validated twice: the first after being created (true) and the second after
        // being released (true)
        verify(factoryMock, Mockito.times(2)).validateObject(adapterMock2);
    }
}
