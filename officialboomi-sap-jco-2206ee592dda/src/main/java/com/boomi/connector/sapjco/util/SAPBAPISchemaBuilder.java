// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sapjco.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.w3c.dom.Document;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.sapjco.SAPJcoConnection;
import com.boomi.util.DOMBuilder2;
import com.boomi.util.SchemaBuilder;
import com.boomi.util.SchemaUtil;
import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoFunctionTemplate;
import com.sap.conn.jco.JCoListMetaData;
import com.sap.conn.jco.JCoTable;

/**
 * @author kishore.pulluru
 *
 */
public class SAPBAPISchemaBuilder extends SAPSchemaBuilder {

	private final Logger logger = Logger.getLogger(SAPBAPISchemaBuilder.class.getName());
    private enum SchemaType {
        REQUEST, RESPONSE;
    }

    /**
     * This method will build and return the response schema for BAPI/BusinessObjects.
     * @param conn
     * @param functionName
     * @return responseSchemaDocument
     * @throws JCoException
     */
    public Document buildResponseSchema(SAPJcoConnection conn, String functionName) throws JCoException {
        return buildSchema(conn, functionName, SchemaType.RESPONSE);
    }

    /**
     * This method will build and returns the request schema for BAPI/BusinessObjects.
     * @param conn
     * @param functionName
     * @return requestSchemaDocument
     * @throws JCoException
     */
    public Document buildRequestSchema(SAPJcoConnection conn, String functionName) throws JCoException {
        return buildSchema(conn, functionName, SchemaType.REQUEST);
    }

    /**
     * This method will build the request/response schema for BAPI/BusinessObjects.
     * @param conn
     * @param functionName
     * @param schemaType
     * @return schemaDocument
     * @throws JCoException
     */
    private static Document buildSchema(SAPJcoConnection conn, String functionName, SchemaType schemaType)
            throws JCoException {
        JCoFunctionTemplate ftemp = conn.getFunctionTemplate(functionName);

        if (ftemp == null) {
            throw new ConnectorException("Could not find BAPI named: " + functionName);
        }

        SchemaBuilder builder = new SchemaBuilder();
        builder.appendSchemaElement(SAPUtil.escape(functionName), null);
        builder.appendComplexType(null);
        builder.appendSequence();

        JCoListMetaData metadata = ftemp.getTableParameterList();
        appendFields(builder, metadata);

        if (schemaType == SchemaType.REQUEST) {
            metadata = ftemp.getImportParameterList();
            appendFields(builder, metadata);
        }
        else if (schemaType == SchemaType.RESPONSE) {
            metadata = ftemp.getExportParameterList();
            appendFields(builder, metadata);
        }
        // The CHANGING parameters are being appended to the SchemaBuilder 'after' the Import/Export parameters since
        // a few customers are using
        // import/export parameters to work around the (earlier) missing implementation for CHANGING.
        // Adding this change here will override the no longer needed duplicates
        JCoListMetaData changingMetadata = ftemp.getChangingParameterList();
        appendFields(builder, changingMetadata);

        return builder.getDocument();
    }

    /**
     * This method Adds elements and creates complex type definitions where needed
     * @param builder
     * @param metadata
     */
    private static void appendFields(SchemaBuilder builder, JCoListMetaData metadata) {
        if (metadata != null) {
            for (int i = 0; i < metadata.getFieldCount(); i++) {
                String minOccurs = metadata.isOptional(i) ? SchemaUtil.OCCURS_ZERO : SchemaUtil.OCCURS_ONE;
                if (metadata.isStructure(i) || metadata.isTable(i)) { // Nested Complex Type
                    appendComplexField(builder, metadata.getType(i), metadata.getName(i), minOccurs,
                            metadata.getRecordTypeName(i), metadata.getRecordMetaData(i), metadata.getDescription(i));
                }
                else {
                    appendField(builder, metadata.getType(i), metadata.getName(i), metadata.getLength(i), minOccurs,
                            SchemaUtil.OCCURS_ONE, metadata.getDescription(i));
                }
            }
        }
    }
    
    
    /**
     * This method will build build business object list with the given filter function name.
     * @param conn
     * @param filter
     * @return businessObjectList
     * @throws JCoException
     */
    public List<Map<String,String>> getBusinessObjects(SAPJcoConnection conn,String filter) throws JCoException{

        JCoFunction func = conn.getFunction("SWO_QUERY_API_OBJTYPES");
        func.getImportParameterList().setValue(SAPJcoConstants.WITH_OBJECT_NAMES, "X");
        func.getImportParameterList().setValue("WITH_TEXTS", "X");
        func.getImportParameterList().setValue("OBJECT_NAME", filter);

        conn.executeFunction(func);

        JCoTable objtypes = func.getTableParameterList().getTable("OBJTYPES");
        List<Map<String,String>> functionList = new ArrayList<>();
       
        if (objtypes != null && objtypes.getNumRows() > 0) {
        	objtypes.firstRow();
            do {
                JCoFunction function = conn.getFunction("SWO_QUERY_API_METHODS");
                function.getImportParameterList().setValue(SAPJcoConstants.WITH_OBJECT_NAMES, "X");
                function.getImportParameterList().setValue(SAPJcoConstants.OBJTYPE, objtypes.getString(SAPJcoConstants.OBJTYPE));
                
                conn.executeFunction(function);

                JCoTable apiMethods = function.getTableParameterList().getTable("API_METHODS");

                if (apiMethods != null && apiMethods.getNumRows() > 0) {
                    apiMethods.firstRow();
                    do {
                    	Map<String,String> functionMap = new HashMap<>();
                    	functionMap.put(SAPJcoConstants.FUNCTION, apiMethods.getString(SAPJcoConstants.FUNCTION));
                    	functionMap.put(SAPJcoConstants.SHORTTEXT, apiMethods.getString(SAPJcoConstants.SHORTTEXT));
                    	functionList.add(functionMap);
                    } while (apiMethods.nextRow());
                }
            } while (objtypes.nextRow());
        }
        
        else {
            throw new ConnectorException("Unable to find any Business Objects that match: " + filter);
        }
        return functionList;
    
    }
    
    /**
     * This method will find all the matched function names with the given filter.
     * @param conn
     * @param filter
     * @return businessObject document
     * @throws JCoException
     */
    public Document buildBusinessObjectsTree(SAPJcoConnection conn, String filter) throws JCoException {
        JCoFunction func = conn.getFunction("SWO_QUERY_API_OBJTYPES");
        func.getImportParameterList().setValue(SAPJcoConstants.WITH_OBJECT_NAMES, "X");
        func.getImportParameterList().setValue("WITH_TEXTS", "X");
        conn.executeFunction(func);

        JCoTable apiMethods = func.getTableParameterList().getTable("OBJTYPES");
        
        DOMBuilder2 builder = new DOMBuilder2(false);
        builder.append("BusinessObjects", null);
        
        if (apiMethods != null && apiMethods.getNumRows() > 0) {
            apiMethods.firstRow();
            do {
                builder.append("businessObject", null).setAttribute("name", apiMethods.getString("OBJNAME"))
                        .setAttribute("desc", apiMethods.getString("SHORTTEXT"));
                buildFunctions(builder, conn, apiMethods.getString(SAPJcoConstants.OBJTYPE));
                builder.toParent();
            } while (apiMethods.nextRow());
        }
        
        else {
            throw new ConnectorException("Unable to find any Business Objects that match: " + filter);
        }
        return builder.getDocument();
    }
    
    
    /**
     * This method will build the functions for each matched API method.
     * @param resultBuilder
     * @param conn
     * @param objtype
     * @throws JCoException
     */
    public void buildFunctions(DOMBuilder2 resultBuilder, SAPJcoConnection conn, String objtype) throws JCoException {
        JCoFunction func = conn.getFunction("SWO_QUERY_API_METHODS");
        func.getImportParameterList().setValue("WITH_OBJECT_NAMES", "X");
        func.getImportParameterList().setValue("OBJTYPE", objtype);
        
        conn.executeFunction(func);

        JCoTable apiMethods = func.getTableParameterList().getTable("API_METHODS");

        if (apiMethods != null && apiMethods.getNumRows() > 0) {
            apiMethods.firstRow();
            do {
                resultBuilder.append("function", null).setAttribute("name", apiMethods.getString("FUNCTION"))
                        .setAttribute("desc", apiMethods.getString("SHORTTEXT")).toParent();
            } while (apiMethods.nextRow());
        }
        else {
           logger.log(Level.SEVERE, "Unable to get API METHODS table");
        }
    }
    
    /**
     * This method will validate the given BAPI/RFM.
     * @param conn
     * @param functionName
     */
    public void validateFunctionName(SAPJcoConnection conn,String functionName){
    	try {
    		JCoFunctionTemplate ftemp = conn.getFunctionTemplate(functionName);

            if (ftemp == null) {
                throw new ConnectorException("Could not find BAPI named: " + functionName);
            }
    	}catch (JCoException e) {
			throw new ConnectorException(e);
		}
    }
}
