// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sapjco.listener;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.connector.api.BasePayload;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.listen.ListenOperation;
import com.boomi.connector.api.listen.Listener;
import com.boomi.connector.api.listen.PayloadBatch;
import com.boomi.connector.sapjco.SAPJcoConnection;
import com.boomi.connector.sapjco.SAPTIDHandler;
import com.boomi.connector.sapjco.util.PayloadDetails;
import com.boomi.connector.sapjco.util.SAPJcoConstants;
import com.boomi.connector.sapjco.util.SAPUtil;
import com.boomi.connector.util.BaseOperation;
import com.boomi.util.CollectionUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.retry.PhasedRetry;
import com.boomi.util.retry.RetryStrategy;
import com.sap.conn.idoc.IDocDocument;
import com.sap.conn.idoc.IDocDocumentList;
import com.sap.conn.idoc.IDocXMLProcessor;
import com.sap.conn.idoc.jco.JCoIDoc;
import com.sap.conn.idoc.jco.JCoIDocHandler;
import com.sap.conn.idoc.jco.JCoIDocHandlerFactory;
import com.sap.conn.idoc.jco.JCoIDocServer;
import com.sap.conn.idoc.jco.JCoIDocServerContext;
import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.server.JCoServerContext;


/**
 * @author a.kumar.samantaray
 *
 */
public class SAPJcoListenOperation extends BaseOperation implements ListenOperation<SAPListenManager>,JCoIDocHandlerFactory,
JCoIDocHandler{

    private static final Logger logger = Logger.getLogger("com.boomi.connector.sap.listener");    
    private static final RetryStrategy RETRY_STRATEGY = new PhasedRetry();
    private final String SERVER_NAME = UUID.randomUUID().toString();
    private SAPServerDataProvider sdp;
    private JCoIDocServer server;
    private SAPJcoConnection conn;
    private SAPTIDHandler tidHandler;
 
    private Listener submitListener;

	public SAPJcoListenOperation(SAPJcoConnection conn) {
		super(conn);
	}

	@Override
	public void start(Listener listener, SAPListenManager listenManager) {
		submitListener=listener;
		boolean successfulStart = false;
        try {
        	conn = this.getConnection();
    		conn.initDestination();
            PropertyMap operationProps = getContext().getOperationProperties();
            String programId = operationProps.getProperty("programId");
            sdp = SAPJcoConnection.getServerDataProvider();
            sdp.registerServer(SERVER_NAME, conn, programId, conn.getDestinationName());
            server = getServerWithRetry();
            server.setIDocHandlerFactory(this);
            tidHandler = new SAPTIDHandler();
            tidHandler.initialize(conn, programId);
            server.setTIDHandler(tidHandler);
            SAPThrowableListener throwList = new SAPThrowableListener();
            server.addServerErrorListener(throwList);
            server.addServerExceptionListener(throwList);
            conn.clearRepositoryCache();
            server.start();
            successfulStart = true;
        } catch (JCoException e) {
            throw new ConnectorException("Unable to register SAP listener", e);
        } catch (Exception e) {
        	throw new ConnectorException("Unable to start the listener", e);
        }
        finally {
            if(!successfulStart) {
            	IOUtil.closeQuietly(conn);
            }
        }
	}
	
	/**
     * This method returns the JCoIDocServer instance using retry function.
     * @return {@link JCoIDocServer}
     * 
     */
	private JCoIDocServer getServerWithRetry() throws JCoException {
        int tries = 0;
        while (true) {
            try {
                return JCoIDoc.getServer(SERVER_NAME);
            }
            catch (JCoException e) {
                ++tries;
                // When you unregister a sever it takes a bit for it to be full unregistered
                // so this retry loop tries to handle exceptions that are caused by us attempting
                // to get a server before the old server has be completely unregistered
                if (RETRY_STRATEGY.shouldRetry(tries, e)) {
                    logger.log(Level.INFO, "Unable to get JCOServer, trying again", e);
                    RETRY_STRATEGY.backoff(tries);
                    continue;
                }
                throw e;
            }
        }
    }
	@Override
	public void stop() {
		 try {
	            if (tidHandler != null) {
	                tidHandler.close();
	                tidHandler = null;
	            }

	            if (sdp != null) {
	                sdp.unregisterServer(SERVER_NAME);
	                sdp = null;
	                //unregistering also stops the server so set it to null
	                server = null;
	            }

	            if (server != null) {
	                server.stop();
	                server = null;
	            }
	        } finally {
	            IOUtil.closeQuietly(conn);
	        }
	}
	
	@Override
	public SAPJcoConnection getConnection() {
		return (SAPJcoConnection) super.getConnection();
	}

	@Override
	 public void handleRequest(JCoServerContext serverContext, IDocDocumentList idocList) {
        boolean success = false;
        PayloadBatch payloadBatch = submitListener.getBatch();
        try {
            for (IDocDocument idoc: CollectionUtil.toIterable(idocList.iterator())) {
                ArrayList<PayloadMetadata> metas = new ArrayList<>();
                PayloadMetadata connTrackData = getContext().createMetadata();
                connTrackData.setTrackedProperty(SAPJcoConstants.TID, serverContext.getTID());
                connTrackData.setTrackedProperty(SAPJcoConstants.IDOCNUMBER, idoc.getIDocNumber());
                connTrackData.setTrackedProperty(SAPJcoConstants.STATUS, idoc.getStatus());
                connTrackData.setTrackedProperty(SAPJcoConstants.IDOCTYPE, idoc.getIDocCompoundType());
                connTrackData.setTrackedProperty(SAPJcoConstants.RECEIVERPORT, idoc.getRecipientPort());
                connTrackData.setTrackedProperty(SAPJcoConstants.PARTNERRECEIVER, idoc.getRecipientPartnerNumber());
                connTrackData.setTrackedProperty(SAPJcoConstants.TESTFLAG, idoc.getTestFlag());
                connTrackData.setTrackedProperty(SAPJcoConstants.SENDERPORT, idoc.getSenderPort());
                connTrackData.setTrackedProperty(SAPJcoConstants.PARTNERTYPE, idoc.getSenderPartnerType());
                connTrackData.setTrackedProperty(SAPJcoConstants.PARTNERNUMBER, idoc.getSenderPartnerNumber());
                connTrackData.setTrackedProperty(SAPJcoConstants.CREATEDDATE, SAPUtil.formatSAPDate(idoc.getCreationDate()));
                connTrackData.setTrackedProperty(SAPJcoConstants.CREATEDTIME, SAPUtil.formatSAPTime(idoc.getCreationTime()));
                connTrackData.setTrackedProperty(SAPJcoConstants.LOGICALMESSAGEVARIENT, idoc.getMessageCode());
                connTrackData.setTrackedProperty(SAPJcoConstants.MESSAGETYPE, idoc.getMessageType());
                connTrackData.setTrackedProperty(SAPJcoConstants.BASICTYPE, idoc.getIDocType());
                connTrackData.setTrackedProperty(SAPJcoConstants.EXTENSION, idoc.getIDocTypeExtension());
                metas.add(connTrackData);
                payloadBatch.add(toPayload(idoc,connTrackData));
                persistPayloadIfRequired(serverContext, idoc);
           }
            payloadBatch.submit();
            success=true;
        }
        catch (Exception e) {
        	payloadBatch.submit(e);
            throw new ConnectorException("Failed while handling SAP IDocs", e);
        } finally {
            if(!success) {
            	logger.log(Level.WARNING, "Failed to write the listened document.");
            }
        }
    }
	  
	/**
	 * Reads and sets the values for @PayloadDetails dao and inserts the idoc payload into db.
	 * @param serverContext
	 * @param idoc
	 */
	private void persistPayloadIfRequired(JCoServerContext serverContext, IDocDocument idoc) {
		if(conn.getTidManagementOptions().equals(SAPJcoConstants.FULL)) {
			PayloadDetails details = new PayloadDetails();
            details.setDocNumber(idoc.getIDocNumber());
            details.setDocType(idoc.getIDocCompoundType());
            details.setIdocType(idoc.getIDocType());
            details.setExtension(idoc.getIDocTypeExtension());
            details.setTid(serverContext.getTID());
            details.setTimestamp(new Timestamp(new Date().getTime()));
            details.setIdoc(getIdocPayload(idoc));
            tidHandler.insertPayloadDetails(details);
        }
	}

	/**
     * This method adds the IDoc and tracked fields in Payload and PayLoadMeta data respectively.
     * @param idoc
     * @param trackData
     * @return {@link Payload}
     * 
     */
	private static Payload toPayload(final IDocDocument idoc, final PayloadMetadata trackData) {
	        return new BasePayload(){
	        	@Override
	            public void writeTo(OutputStream out) throws IOException {
	                IDocXMLProcessor xmlProcessor = JCoIDoc.getIDocFactory().getIDocXMLProcessor();
	                OutputStreamWriter writer = new OutputStreamWriter(out, StringUtil.UTF8_CHARSET);
	                xmlProcessor.render(idoc, writer, StringUtil.UTF8_CHARSET.name());
	                writer.flush();
	            }
	            @Override
	            public  PayloadMetadata getMetadata() {
					return trackData;
	            	
	            }
	        };
	    }
	
	@Override
	public JCoIDocHandler getIDocHandler(JCoIDocServerContext serverContext) {
		
		  return this;
	}
	
	/**
	 * This method return the xml idocument from IDocDocument.
	 * @param idoc
	 * @return xmlIdoc
	 */
	private String getIdocPayload(final IDocDocument idoc) {
		IDocXMLProcessor xmlProcessor = JCoIDoc.getIDocFactory().getIDocXMLProcessor();
		//each iDoc will not exceed 64kb.
		return xmlProcessor.render(idoc);
	}
}
