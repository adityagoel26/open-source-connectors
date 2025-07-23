// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sapjco.operation;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PayloadUtil;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.sapjco.SAPJcoConnection;
import com.boomi.connector.sapjco.util.SAPJcoConstants;
import com.boomi.connector.sapjco.util.SAPUtil;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.util.IOUtil;
import com.sap.conn.idoc.IDocDocument;
import com.sap.conn.idoc.IDocDocumentIterator;
import com.sap.conn.idoc.IDocDocumentList;
import com.sap.conn.idoc.IDocFactory;
import com.sap.conn.idoc.IDocIllegalTypeException;
import com.sap.conn.idoc.IDocParseException;
import com.sap.conn.idoc.IDocRepository;
import com.sap.conn.idoc.IDocXMLProcessor;
import com.sap.conn.idoc.jco.JCoIDoc;
import com.sap.conn.jco.JCoDestination;
import com.sap.conn.jco.JCoException;

/**
 * @author a.kumar.samantaray
 *
 */
public class SAPJCoSendOperation extends BaseUpdateOperation implements Operation {

	public SAPJCoSendOperation(SAPJcoConnection conn) {
		super(conn);
	}

	@Override
	protected void executeUpdate(UpdateRequest request, OperationResponse response) {
		List<ObjectData> trackedData = new ArrayList<>();
		for (ObjectData oData : request) {
			trackedData.add(oData);
		}
		this.executeSendOperation(trackedData, response);
	}

	/**
	 * This method will execute the send operation request and process the input
	 * data.
	 * 
	 * @param request
	 * @param response
	 * 
	 */
	protected void executeSendOperation(List<ObjectData> request, OperationResponse response) {
		SAPJcoConnection sapcon = null;
		try {
			sapcon = getConnection();
			sapcon.initDestination();
			IDocRepository iDocRepo = this.getIDocRepository(sapcon);
			String tid = this.createTID(sapcon);
			IDocFactory iDocFact = JCoIDoc.getIDocFactory();
			IDocXMLProcessor processor = iDocFact.getIDocXMLProcessor();
			IDocDocumentList iDocList = parseIdocs(request, response, processor, iDocRepo, tid);
			if (null != iDocList) {
				this.sendIdocSap(iDocList, sapcon, request, response, tid);
			}
		} catch (Exception e) {
			sendApplicationErrorResponse(request, response, "0", e);
		} finally {
			IOUtil.closeQuietly(sapcon);
		}
	}
	
	/**
	 * This method will parse the input IDocs and return.
	 * @param request
	 * @param response
	 * @param processor
	 * @param iDocRepo
	 * @param tid
	 * @return IDocDocumentList
	 */
	private IDocDocumentList parseIdocs(List<ObjectData> request, OperationResponse response, IDocXMLProcessor processor, IDocRepository iDocRepo, String tid) {
		IDocDocumentList iDocList = null;
		for (ObjectData data : request) {
			InputStream ins = null;
			try {
				ins = data.getData();
				IDocDocumentList tempList = processor.parse(iDocRepo, ins);
				if (iDocList == null) {
					iDocList = tempList;
					continue;
				}

				if (iDocList.getIDocType().equals(tempList.getIDocType())) {
					IDocDocumentIterator iDocIter = tempList.iterator();
					while (iDocIter.hasNext()) {
						iDocList.add(iDocIter.next());
					}
				} else {
					String exceptionMsg = "Inbound documents need to be the same IDocType (Found: "
							+ tempList.getIDocType() + " Expected: " + iDocList.getIDocType() + " Tid : " + tid
							+ " )";
					response.addErrorResult(data, OperationStatus.FAILURE, "0", exceptionMsg,
							new ConnectorException(exceptionMsg));
				}

			} catch (IDocParseException | IOException | IDocIllegalTypeException e) {
				response.getLogger().log(Level.SEVERE, e.getMessage() + " Tid : " + tid);
				response.addErrorResult(data, OperationStatus.FAILURE, "0", e.getMessage(), new ConnectorException(e.getMessage() + ", Tid : "+ tid));
			} finally {
				IOUtil.closeQuietly(ins);
			}
		}
		return iDocList;
	}

	/**
	 * This method sends the list of IDoc to SAP System and error cases to
	 * ResponseUtil/Payload.
	 * 
	 * @param iDocList
	 * @param sapcon
	 * @param bdToSend
	 * @param response
	 * @param tid
	 * 
	 */
	private void sendIdocSap(IDocDocumentList iDocList, SAPJcoConnection sapcon, List<ObjectData> bdToSend,
			OperationResponse response, String tid) {
		try {
			response.getLogger().log(Level.INFO,
					"Sending " + iDocList.getNumDocuments() + " iDocs of type " + iDocList.getIDocType());
			JCoIDoc.send(iDocList, '0', sapcon.getDestination(), tid);
			// tid added document property in response
			sapcon.getDestination().confirmTID(tid);
			sendResponse(bdToSend, response, OperationStatus.SUCCESS, SAPJcoConstants.SUCCESS_RES_CODE,
					SAPJcoConstants.SUCCESS_RES_MSG, tid, iDocList);
		} catch (JCoException e) {
			response.getLogger().log(Level.SEVERE, "Tid :" + tid + " Exception : " + e.getMessage());
			sendResponse(bdToSend, response, OperationStatus.FAILURE, "0", e.getMessage(), tid, iDocList);
			ResponseUtil.addExceptionFailures(response, bdToSend, e);
		}
	}

	/**
	 * This method adds the response and track properties in Payload.
	 * @param data
	 * @param response
	 * @param status
	 * @param statusCode
	 * @param statusMsg
	 * @param tid
	 * @param iDocList
	 */
	public void sendResponse(List<ObjectData> data, OperationResponse response, OperationStatus status, String statusCode, String statusMsg,
			String tid, IDocDocumentList iDocList) {
		boolean consolidateOutput =  false;
		try {
			consolidateOutput = getContext().getOperationProperties().getBooleanProperty("consolidateOutput");
		}catch(Exception e) {
			//added new field, in case of exception while getting the property continue execution with value as false.
		}
		
		if(consolidateOutput) {
			String output =  iDocList.getIDocType();
			PayloadMetadata connTrackData = getContext().createMetadata();
			connTrackData.setTrackedProperty(SAPJcoConstants.TID, tid);
			connTrackData.setTrackedProperty(SAPJcoConstants.BASICTYPE, iDocList.getIDocType());
	        connTrackData.setTrackedProperty(SAPJcoConstants.EXTENSION, iDocList.getIDocTypeExtension());
			response.addCombinedResult(data, status, statusCode, statusMsg, PayloadUtil.toPayload(output, connTrackData));
		}else {
			sendIndividualResponse(data, response, status, statusCode, statusMsg, tid, iDocList);
		}
	}

	/**
	 * This method adds the response and track properties for individual iDocs in Payload.
	 * @param data
	 * @param response
	 * @param status
	 * @param statusCode
	 * @param statusMsg
	 * @param tid
	 * @param iDocList
	 */
	public void sendIndividualResponse(List<ObjectData> data, OperationResponse response, OperationStatus status,
			String statusCode, String statusMsg, String tid, IDocDocumentList iDocList) {
		int i = 0;
		String output =  iDocList.getIDocType();
		for (TrackedData trackedData : data) {
			IDocDocument idoc = iDocList.get(i);
			PayloadMetadata connTrackData = getContext().createMetadata();
			connTrackData.setTrackedProperty(SAPJcoConstants.TID, tid);
			connTrackData.setTrackedProperty(SAPJcoConstants.IDOCNUMBER, idoc.getIDocNumber());
			connTrackData.setTrackedProperty(SAPJcoConstants.STATUS, idoc.getStatus());
			connTrackData.setTrackedProperty(SAPJcoConstants.IDOCTYPE, idoc.getIDocCompoundType());
			connTrackData.setTrackedProperty(SAPJcoConstants.RECEIVERPORT, idoc.getRecipientPort());
			connTrackData.setTrackedProperty(SAPJcoConstants.PARTNERRECEIVER, idoc.getRecipientPartnerNumber());
			connTrackData.setTrackedProperty(SAPJcoConstants.TESTFLAG, idoc.getTestFlag());
			connTrackData.setTrackedProperty(SAPJcoConstants.SENDERPORT, idoc.getSenderPort());
			connTrackData.setTrackedProperty(SAPJcoConstants.PARTNERTYPE, idoc.getSenderPartnerType());
			connTrackData.setTrackedProperty(SAPJcoConstants.PARTNERNUMBER, idoc.getSenderPartnerNumber());
			connTrackData.setTrackedProperty(SAPJcoConstants.CREATEDDATE,
					SAPUtil.formatSAPDate(idoc.getCreationDate()));
			connTrackData.setTrackedProperty(SAPJcoConstants.CREATEDTIME,
					SAPUtil.formatSAPTime(idoc.getCreationTime()));
			connTrackData.setTrackedProperty(SAPJcoConstants.LOGICALMESSAGEVARIENT, idoc.getMessageCode());
			connTrackData.setTrackedProperty(SAPJcoConstants.MESSAGETYPE, idoc.getMessageType());
			connTrackData.setTrackedProperty(SAPJcoConstants.BASICTYPE, idoc.getIDocType());
			connTrackData.setTrackedProperty(SAPJcoConstants.EXTENSION, idoc.getIDocTypeExtension());
			response.addResult(trackedData, status, statusCode, statusMsg,
					PayloadUtil.toPayload(output, connTrackData));
			i++;
		}
	}

	/**
	 * This method will send the application error response to the connector.
	 * 
	 * @param data
	 * @param response
	 * @param statusCode
	 * @param errorMsg
	 */
	public static void sendApplicationErrorResponse(List<ObjectData> data, OperationResponse response,
			String statusCode, Exception e) {
		for (TrackedData trackedData : data) {
			response.addErrorResult(trackedData, OperationStatus.APPLICATION_ERROR, statusCode, e.getMessage(),
					new ConnectorException(e));
		}
	}

	/**
	 * This method creates a TID from SAP.
	 * 
	 * @param sapcon
	 * @return {@link String}
	 * 
	 */
	private String createTID(SAPJcoConnection sapcon) {
		try {
			return sapcon.getDestination().createTID();
		} catch (JCoException e) {
			throw new ConnectorException("Unable to create TID", (Throwable) e);
		}
	}

	/**
	 * This method will calls the SAP system using the Destination and returns the
	 * IDoc Repository.
	 * 
	 * @param sapcon
	 * @return {@link IDocRepository}
	 * 
	 */
	private IDocRepository getIDocRepository(SAPJcoConnection sapcon) {

		try {
			return JCoIDoc.getIDocRepository((JCoDestination) sapcon.getDestination());
		} catch (JCoException e) {
			throw new ConnectorException("Unable to connect to SAP", (Throwable) e);
		}
	}

	@Override
	public SAPJcoConnection getConnection() {
		return (SAPJcoConnection) super.getConnection();
	}

}
