// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sapjco.listener;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.sap.conn.jco.server.JCoServer;
import com.sap.conn.jco.server.JCoServerContextInfo;
import com.sap.conn.jco.server.JCoServerErrorListener;
import com.sap.conn.jco.server.JCoServerExceptionListener;

/**
 * @author a.kumar.samantaray
 *
 */
public class SAPThrowableListener
implements JCoServerErrorListener,
JCoServerExceptionListener {
    private static final Logger logger = Logger.getLogger("com.boomi.connector.sap.listener");

    /**
     * This method executes while the exception occurs in server while listening.
     * @param server
     * @param connectionId
     * @param serverContext
     * @param exception
     * 
     */
    public void serverExceptionOccurred(JCoServer server, String connectionId, JCoServerContextInfo serverContext, Exception exception) {
        logger.log(Level.SEVERE, "Server Exception occured on {0}, connection : {1}" , new String[]{server.getProgramID(), connectionId} );
    }

    /**
     * This method executes while the Error occurs in server while listening.
     * @param server
     * @param connectionId
     * @param serverContext
     * @param exception
     * 
     */
    public void serverErrorOccurred(JCoServer server, String connectionId, JCoServerContextInfo serverContext, Error error) {
        logger.log(Level.SEVERE, "Server Error occured on {0} , connection :{1} ", new String[]{server.getProgramID(), connectionId} );
    }
}

