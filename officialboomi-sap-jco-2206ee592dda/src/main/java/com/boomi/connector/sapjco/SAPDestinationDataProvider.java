// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sapjco;

import com.sap.conn.jco.ext.DestinationDataEventListener;
import com.sap.conn.jco.ext.DestinationDataProvider;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author kishore.pulluru
 *
 */
public class SAPDestinationDataProvider
implements DestinationDataProvider {
    private DestinationDataEventListener eventListener;
    private ConcurrentHashMap<String, Properties> destinations = new ConcurrentHashMap<>();

    /* (non-Javadoc)
     * @see com.sap.conn.jco.ext.DestinationDataProvider#getDestinationProperties(java.lang.String)
     */
    public Properties getDestinationProperties(String destinationName) {
        return this.destinations.get(destinationName);
    }

    /* (non-Javadoc)
     * @see com.sap.conn.jco.ext.DestinationDataProvider#setDestinationDataEventListener(com.sap.conn.jco.ext.DestinationDataEventListener)
     */
    public void setDestinationDataEventListener(DestinationDataEventListener el) {
        this.eventListener = el;
    }

    /* (non-Javadoc)
     * @see com.sap.conn.jco.ext.DestinationDataProvider#supportsEvents()
     */
    public boolean supportsEvents() {
        return true;
    }

    /**
     * This method will register the destination with provided destination name and properties.
     * @param destinationName
     * @param props
     */
    public void registerDestination(String destinationName, Properties props) {
        this.destinations.put(destinationName, props);
    }

    /**
     * This method will unregister the destination with provided destination name.
     * @param destinationName
     */
    public void unregisterDestination(String destinationName) {
        if (this.destinations.remove(destinationName) != null) {
            this.eventListener.deleted(destinationName);
        }
    }
}

