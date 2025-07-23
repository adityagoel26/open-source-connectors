// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sapjco.operation;

import java.io.InputStream;
import java.io.StringReader;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PayloadUtil;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.sapjco.SAPJcoConnection;
import com.boomi.connector.sapjco.util.SAPJcoConstants;
import com.boomi.connector.sapjco.util.SAPUtil;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.util.DOMBuilder2;
import com.boomi.util.DOMUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.XMLUtil;
import com.sap.conn.jco.AbapException;
import com.sap.conn.jco.ConversionException;
import com.sap.conn.jco.JCoContext;
import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoFunctionTemplate;
import com.sap.conn.jco.JCoListMetaData;
import com.sap.conn.jco.JCoMetaData;
import com.sap.conn.jco.JCoParameterList;
import com.sap.conn.jco.JCoRecord;
import com.sap.conn.jco.JCoStructure;
import com.sap.conn.jco.JCoTable;

/**
 * @author kishore.pulluru
 *
 */
public class SAPJcoExecuteOperation extends BaseUpdateOperation {

	private static final Logger logger = Logger.getLogger(SAPJcoExecuteOperation.class.getName());

	public SAPJcoExecuteOperation(SAPJcoConnection conn) {
		super(conn);
	}

	@Override
	protected void executeUpdate(UpdateRequest request, OperationResponse response) {
		SAPJcoConnection con = null;
		try {
			con = getConnection();
			con.initDestination();
			handleDocuments(request, con, response);
		}catch(Exception e) {
			ResponseUtil.addExceptionFailures(response, request, e);
		}finally {
			IOUtil.closeQuietly(con);
		}
	}

	/**
	 *  This method will handle the each document, builds the request and responses. 
	 * @param trackedData
	 * @param conn
	 * @param request
	 * @param response
	 */
	private void handleDocuments(UpdateRequest request, SAPJcoConnection conn, OperationResponse response) {
		String functionName = getContext().getObjectTypeId();
		InputStream inputdata = null;
		boolean commitTransaction = getContext().getOperationProperties()
				.getBooleanProperty(SAPJcoConstants.COMMIT_TXN);
		
		for(ObjectData objectData: request) {
			try {
				JCoContext.begin(conn.getDestination());
				// SAP suggests you get a new function each time to make
				// sure that
				// all parameters are refreshed
				JCoFunction function = conn.getFunction(functionName);

				if (function == null) {
					throw new ConnectorException("BAPI " + functionName + " not found");
				}

				JCoFunctionTemplate temp = function.getFunctionTemplate();
				inputdata = objectData.getData();
				Document inputDoc = XMLUtil.parseXML(inputdata);

				populateParamList(function.getTableParameterList(), temp.getTableParameterList(),
						inputDoc.getDocumentElement());
				populateParamList(function.getImportParameterList(), temp.getImportParameterList(),
						inputDoc.getDocumentElement());
				populateParamList(function.getChangingParameterList(), temp.getChangingParameterList(),
						inputDoc.getDocumentElement());

				conn.executeFunction(function);
				checkForErrors(functionName, function);

				if (commitTransaction) {
					conn.commitBAPITx();
				}
				response.addResult(objectData, OperationStatus.SUCCESS, SAPJcoConstants.SUCCESS_RES_CODE, SAPJcoConstants.SUCCESS_RES_MSG, PayloadUtil.toPayload(buildResponse(function)));
			} catch (Exception ex) {
				response.addErrorResult(objectData, OperationStatus.APPLICATION_ERROR, (String)"0", ex.getMessage(), new ConnectorException(ex.getMessage()));
			} finally {
				IOUtil.closeQuietly(inputdata);
				try {
					JCoContext.end(conn.getDestination());
				} catch (JCoException e) {
					logger.log(Level.SEVERE, "JCoException while ending destination {0}", e.getMessage());
				}
			}
		}
	}

	/**
	 * Checks for any errors for each document once the execute operation is completed.
	 * @param functionName
	 * @param function
	 */
	private void checkForErrors(String functionName, JCoFunction function) {
		AbapException[] errors = (function != null ? function.getExceptionList() : null);

		// To handle the case of more than one error
		StringBuilder errorMessages = new StringBuilder();
		if (errors != null && errors.length > 0) {
			// We have errors
			for (AbapException error : errors) {
				// Send exception trace to container logs
				if (null != error) {
					errorMessages.append(error.getMessage());
					errorMessages.append("\n");
				}
			}
			// Log error messages instead of a hard failure
			logger.log(Level.WARNING, "Functional Exception(s) Occurred for BAPI/RFM : {0} : {1}",new Object[] { functionName, errorMessages });
		}
	}

	/**
	 * This method will parse and populate the response from sap.
	 * @param paramList
	 * @param metadata
	 * @param el
	 */
	private void populateParamList(JCoParameterList paramList, JCoListMetaData metadata, Element el) {
		if (metadata == null) {
			return;
		}
		for (Element child : DOMUtil.elementIterable(el.getChildNodes())) {
			String name = DOMUtil.getLocalName(child);

			if (!metadata.hasField(name)) {
				// just skip it
				continue;
			}

			if (metadata.isStructure(name) && isStructureEl(child)) {
				// structure
				setStructureParams(child, paramList);
			} else if (metadata.isTable(name) && isTableEl(child)) {
				// table
				setTableParams(child, paramList);
			} else if (isFieldEl(child)) {
				// field
				setValue(paramList, child, metadata);
			} else {
				logger.log(Level.SEVERE, "Invalid input ({0}) it does not appear to be a table, structure or field",name);
			}
		}
	}

	/**
	 * This method will set the parameters for structure parameters.
	 * @param structureEl
	 * @param paramList
	 */
	private void setStructureParams(Element structureEl, JCoParameterList paramList) {
		JCoStructure struct = paramList.getStructure(DOMUtil.getLocalName(structureEl));
		setStructureValue(structureEl, struct);
	}

	/**
	 * This method will set the parameters for table parameters.
	 * @param tableEl
	 * @param paramList
	 */
	private void setTableParams(Element tableEl, JCoParameterList paramList) {
		JCoTable table = paramList.getTable(DOMUtil.getLocalName(tableEl));
		setTableValue(tableEl, table);
	}

	/**
	 * This method sets the values for parameters based on metadata type.
	 * @param record
	 * @param recordEl
	 * @param metadata
	 */
	private void setValue(JCoRecord record, Element recordEl, JCoMetaData metadata) {
		String val = DOMUtil.getTextContent(recordEl);
		String name = DOMUtil.getLocalName(recordEl);
		if (StringUtil.isEmpty(val)) {
			return;
		}

		switch (metadata.getType(name)) {
		case JCoMetaData.TYPE_FLOAT:
			try {
				record.setValue(name, Double.parseDouble(val));
			} catch (NumberFormatException e) {
				logger.log(Level.SEVERE,  " {0} must be a Double, ignoring...", name );
			}
			break;
		case JCoMetaData.TYPE_INT:
		case JCoMetaData.TYPE_INT1:
		case JCoMetaData.TYPE_INT2:
			try {
				record.setValue(name, Integer.parseInt(val));
			} catch (NumberFormatException e) {
				logger.log(Level.SEVERE, "{0} must be an Integer, ignoring...",name);
			}
			break;
		case JCoMetaData.TYPE_STRUCTURE:
			JCoStructure struct = record.getStructure(name);
			setStructureValue(recordEl, struct);
			break;
		case JCoMetaData.TYPE_TABLE:
			JCoTable table = record.getTable(name);
			setTableValue(recordEl, table);
			break;
		default:
			try {
				record.setValue(name, val);
			} catch (ConversionException e) {
				LogUtil.severe(logger, e, "Invalid input encountered. For type {%s} value {%s}.", name, val);
				throw e;
			}
			break;
		}
	}

	/**
	 * This method will sets the value for parameters from jcoStructure.
	 * @param structureEl
	 * @param struct
	 */
	private void setStructureValue(Element structureEl, JCoStructure struct) {
		for (Element child : DOMUtil.elementIterable(structureEl.getChildNodes())) {
			setValue(struct, child, struct.getMetaData());
		}
	}

	/**
	 * This method will sets the value for parameters from jcoTable.
	 * @param tableEl
	 * @param table
	 */
	private void setTableValue(Element tableEl, JCoTable table) {
		for (Element items : DOMUtil.elementIterable(tableEl.getChildNodes(), SAPJcoConstants.ITEM)) {
			table.appendRow();
			for (Element child : DOMUtil.elementIterable(items.getChildNodes())) {
				setValue(table, child, table.getMetaData());
			}
		}
	}

	/**
	 * This method will return true if the given element is structure element.
	 * @param structEl
	 * @return boolean
	 */
	private static boolean isStructureEl(Element structEl) {
		if (structEl.hasChildNodes()) {
			return DOMUtil.getFirstElementChild(structEl, SAPJcoConstants.ITEM) == null;
		}
		return false;
	}

	/**
	 * This method will return true if the given element is table element.
	 * @param tableEl
	 * @return boolean
	 */
	private static boolean isTableEl(Element tableEl) {
		if (tableEl.hasChildNodes()) {
			return DOMUtil.getFirstElementChild(tableEl, SAPJcoConstants.ITEM) != null;
		}
		return false;
	}

	/**
	 * This method will return true if the given element is field element.
	 * @param fieldEl
	 * @return
	 */
	private static boolean isFieldEl(Element fieldEl) {
		return !DOMUtil.hasChildElements(fieldEl);
	}

	/**
	 * This method will build the response from jcoFunction.
	 * @param function
	 * @return document
	 */
	private static Document buildResponse(JCoFunction function) {
		DOMBuilder2 builder = new DOMBuilder2();
		builder.append(SAPUtil.escape(function.getName()), null);
		appendChildren(builder, function.getTableParameterList());
		appendChildren(builder, function.getChangingParameterList());
		appendChildren(builder, function.getExportParameterList());
		return builder.getDocument();
	}

	/**
	 * This method is helper method to build response from jcoFunction and builds response by parsing response xml's.
	 * @param builder
	 * @param paramList
	 */
	private static void appendChildren(DOMBuilder2 builder, JCoParameterList paramList) {
		if (paramList != null) {
			try {
				String xml = paramList.toXML();
				Document doc = XMLUtil.parseXML(new InputSource(new StringReader(xml)));
				for (Node child : DOMUtil.nodeListIterable(doc.getDocumentElement().getChildNodes())) {
					builder.appendImported(child).toParent();
				}
			} catch (Exception e) {
				throw new ConnectorException("Unable to read response from SAP", e);
			}
		}
	}

	@Override
	public SAPJcoConnection getConnection() {
		return (SAPJcoConnection) super.getConnection();
	}
}