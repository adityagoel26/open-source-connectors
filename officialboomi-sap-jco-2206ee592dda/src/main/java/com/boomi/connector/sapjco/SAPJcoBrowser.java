// Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sapjco;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.XMLConstants;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;

import com.boomi.connector.api.ConnectionTester;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.sapjco.util.SAPBAPISchemaBuilder;
import com.boomi.connector.sapjco.util.SAPIDocSchemaBuilder;
import com.boomi.connector.sapjco.util.SAPJcoConstants;
import com.boomi.connector.util.BaseBrowser;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.sap.conn.jco.JCoException;

/**
 * @author kishore.pulluru
 *
 */
public class SAPJcoBrowser extends BaseBrowser implements ConnectionTester{

	private static final Logger logger = Logger.getLogger(SAPJcoBrowser.class.getName());

	@SuppressWarnings("unchecked")
	protected SAPJcoBrowser(SAPJcoConnection conn) {
		super(conn);
	}

	@Override
	public ObjectDefinitions getObjectDefinitions(String objectTypeId, Collection<ObjectDefinitionRole> roles) {
		String optype = getContext().getOperationType().toString();
		String customOperation = getContext().getCustomOperationType();
		optype = StringUtil.isBlank(customOperation) ? optype : customOperation;
		ObjectDefinitions objdefs = new ObjectDefinitions();

		if (optype.equalsIgnoreCase(SAPJcoConstants.EXECUTE)) {
			objdefs = getObjectDefinitionsBOBAPI(objectTypeId, roles);
		} else if (optype.equalsIgnoreCase(SAPJcoConstants.SEND) || optype.equalsIgnoreCase(SAPJcoConstants.LISTEN)) {
			objdefs = getObjectDefinitionsIDOCSendOrListen(objectTypeId);
		}
		return objdefs;
	}

	/**
	 * This method will build the object definitions/schema of IDOC for send and listen operations.
	 * @param objectTypeId
	 * @return ObjectDefinitions
	 */
	private ObjectDefinitions getObjectDefinitionsIDOCSendOrListen(String objectTypeId) {
		ObjectDefinitions objdefs = new ObjectDefinitions();
		PropertyMap operationProps = getContext().getOperationProperties();
		SAPJcoConnection con = null;
		ObjectDefinition objdef = new ObjectDefinition();
		objdef.setElementName("");
		try {
			con = getConnection();
			con.initDestination();
			SAPIDocSchemaBuilder schemaIdocBuilder = new SAPIDocSchemaBuilder();
			objdef.setSchema(schemaIdocBuilder.buildIdocSchema(con, objectTypeId, operationProps).getDocumentElement());
		} catch (JCoException e) {
			throw new ConnectorException("Exception while building IDOC schema" + e.getMessage());
		} finally {
			IOUtil.closeQuietly(con);
		}
		objdef.setOutputType(ContentType.XML);
		objdef.setInputType(ContentType.XML);
		objdefs.getDefinitions().add(objdef);
		return objdefs;
	}

	/**
	 * This method will build the object definitions/schema of BAPI for Execute operation.
	 * @param objectTypeId
	 * @param roles
	 * @return ObjectDefinitions
	 */
	private ObjectDefinitions getObjectDefinitionsBOBAPI(String objectTypeId, Collection<ObjectDefinitionRole> roles) {
		SAPBAPISchemaBuilder schemaBuilder = new SAPBAPISchemaBuilder();
		ObjectDefinitions objdefs = new ObjectDefinitions();
		SAPJcoConnection con = null;
		try {
			con = getConnection();
			con.initDestination();
			for (ObjectDefinitionRole role : roles) {
				ObjectDefinition objdef = new ObjectDefinition();
				switch (role) {
				case INPUT:
					objdef.setElementName("");
					try {
						Document doc = schemaBuilder.buildRequestSchema(con, objectTypeId);
						writeXmlDocumentToXmlFile(doc);
						objdef.setSchema(doc.getDocumentElement());
					} catch (JCoException e) {
						throw new ConnectorException("Exception while building Input BAPI schema:" + e.getMessage());
					}
					objdef.setOutputType(ContentType.XML);
					objdef.setInputType(ContentType.XML);
					objdefs.getDefinitions().add(objdef);
					break;
				case OUTPUT:
					objdef.setElementName("");
					try {
						objdef.setSchema(schemaBuilder.buildResponseSchema(con, objectTypeId).getDocumentElement());
					} catch (JCoException e) {
						throw new ConnectorException("Exception while building Output BAPI schema:" + e.getMessage());
					}
					objdef.setOutputType(ContentType.XML);
					objdef.setInputType(ContentType.XML);
					objdefs.getDefinitions().add(objdef);
					break;
				default:
					break;
				}
			}
		} finally {
			IOUtil.closeQuietly(con);
		}

		return objdefs;
	}

	@Override
	public ObjectTypes getObjectTypes() {
		String optype = getContext().getOperationType().toString();
		ObjectTypes objtypes = new ObjectTypes();
		List<ObjectType> objTypeList = optype.equalsIgnoreCase(SAPJcoConstants.EXECUTE) ? getObjectTypesExecute()
				: getObjectTypesListen();
		objtypes.getTypes().addAll(objTypeList);
		return objtypes;
	}

	/**
	 * This method will return the object types for Listen operation.
	 * @return List of ObjectTypes.
	 */
	private List<ObjectType> getObjectTypesListen() {
		PropertyMap operationProps = getContext().getOperationProperties();
		String basetype = operationProps.getProperty("basetype");
		return getObjectTypesIDOC(basetype);
	}

	/**
	 * This method will return the object types for Execute operation.
	 * @return List of ObjectTypes.
	 */
	private List<ObjectType> getObjectTypesExecute() {
		List<ObjectType> objTypeList = new ArrayList<>();
		PropertyMap operationProps = getContext().getOperationProperties();
		String function = operationProps.getProperty(SAPJcoConstants.FUNCTION_NAME);
		String functionType = operationProps.getProperty(SAPJcoConstants.FUNCTION_TYPE);
		String basetype = operationProps.getProperty("basetype");
		switch (functionType) {
		case SAPJcoConstants.BUSINESS_OBJECT:
			return getObjectTypesBO(function);
		case SAPJcoConstants.BAPI:
			return getObjectTypesBAPI(function);
		case SAPJcoConstants.IDOC:
			return getObjectTypesIDOC(basetype);
		default:
			return objTypeList;
		}
	}

	/**
	 * This method will build and return object types for IDoc.
	 * @param basetype
	 * @return List of ObjectTypes
	 */
	private List<ObjectType> getObjectTypesIDOC(String basetype) {
		List<ObjectType> objTypeList = new ArrayList<>();
		PropertyMap operationProps = getContext().getOperationProperties();
		SAPIDocSchemaBuilder schemaBuilder = new SAPIDocSchemaBuilder();
		SAPJcoConnection con = null;
		try {
			con = this.getConnection();
			con.initDestination();
			schemaBuilder.validateIDocType(con, basetype, operationProps);
			ObjectType funObj = new ObjectType();
			funObj.setId(basetype);
			funObj.setLabel(basetype);
			objTypeList.add(funObj);
		}finally {
			IOUtil.closeQuietly(con);
		}
		return objTypeList;
	}

	/**
	 * This method will build and return object types for BAPI.
	 * @param function
	 * @return List of ObjectTypes
	 */
	private List<ObjectType> getObjectTypesBAPI(String function) {
		List<ObjectType> objTypeList = new ArrayList<>();
		SAPBAPISchemaBuilder schemaBuilder = new SAPBAPISchemaBuilder();
		SAPJcoConnection con = null;
		try {
			con = this.getConnection();
			con.initDestination();
			schemaBuilder.validateFunctionName(con, function);
			ObjectType funObj = new ObjectType();
			funObj.setId(function);
			funObj.setLabel(function);
			objTypeList.add(funObj);
		}finally {
			IOUtil.closeQuietly(con);
		}
		return objTypeList;
	}

	/**
	 * This method will build and return object types for Business Object.
	 * @param function
	 * @return List of ObjectTypes
	 */
	private List<ObjectType> getObjectTypesBO(String function) {
		List<ObjectType> objTypeList = new ArrayList<>();
		SAPJcoConnection con = null;
		try {
			con = this.getConnection();
			con.initDestination();
			
			if (StringUtil.isBlank(function)) {
				throw new ConnectorException("Function name should not be empty");
			}
			SAPBAPISchemaBuilder schemaBuilder = new SAPBAPISchemaBuilder();
			List<Map<String, String>> functionList = null;
			try {
				functionList = schemaBuilder.getBusinessObjects(con, function);
			} catch (JCoException e) {
				throw new ConnectorException("JCOException while building business objects :" + e.getMessage());
			}
			for (Map<String, String> funMap : functionList) {
				ObjectType funObj = new ObjectType();
				funObj.setId(funMap.get(SAPJcoConstants.FUNCTION));
				funObj.setLabel(funMap.get(SAPJcoConstants.FUNCTION) + SAPJcoConstants.DELIMITER
						+ funMap.get(SAPJcoConstants.SHORTTEXT));
				objTypeList.add(funObj);
			}
		} finally {
			IOUtil.closeQuietly(con);
		}

		return objTypeList;
	}
	
	/**
	 * This method will transform document to XML file.
	 * @param xmlDocument
	 */
	private static void writeXmlDocumentToXmlFile(Document xmlDocument)
	{
	    TransformerFactory tf = TransformerFactory.newInstance();
	    Transformer transformer;
	    try {
	    	tf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
	        transformer = tf.newTransformer();
	        StringWriter writer = new StringWriter();
	 
	        //transform document to string 
	        transformer.transform(new DOMSource(xmlDocument), new StreamResult(writer));
	    } 
	    catch (Exception e) 
	    {
	    	logger.log(Level.SEVERE,"Exception at writeXmlDocumentToXmlFile() : {0}", e.getMessage());
	    }
	}

	@Override
	public SAPJcoConnection getConnection() {
		return (SAPJcoConnection) super.getConnection();
	}

	@Override
	public void testConnection() {
		try(SAPJcoConnection con = getConnection()) {
			con.initDestination();
			con.getDestination().getRepository();
		}catch(Exception e) {
			throw new ConnectorException(e.getMessage());
		}
	}
}