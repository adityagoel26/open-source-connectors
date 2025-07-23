// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sapjco.util;

import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.sapjco.SAPJcoConnection;
import com.boomi.util.DOMUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.NumberUtil;
import com.boomi.util.SchemaBuilder;
import com.boomi.util.StringUtil;
import com.boomi.util.XMLUtil;
import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoTable;
/**
 * @author kishore.pulluru
 *
 */
public class SAPIDocSchemaBuilder extends SAPSchemaBuilder {
	private static final Logger logger = Logger.getLogger(SAPIDocSchemaBuilder.class.getName());
	private static final String EDI_DC40_XSD = "/EDI_DC40.xsd";
	private static final Object TOP_LEVEL_MARK = new Object();

	/**
     * This method builds the Schema for IDOC send and Listen operation.
     * @param conn
     * @param functionName
     * @param operationProps
     * @return {@link Document}
     * 
     */
	public Document buildIdocSchema(SAPJcoConnection conn, String functionName, PropertyMap operationProps)
			throws JCoException {
		String profileRoot = SAPUtil.escape(functionName);
		String extension = operationProps.getProperty("extension");
		SchemaBuilder builder = new SchemaBuilder();
		try {
			JCoFunction func = conn.getFunction("IDOCTYPE_READ_COMPLETE");
			func.getImportParameterList().setValue("PI_IDOCTYP", functionName);
			if (!StringUtil.isEmpty(extension)) {
				func.getImportParameterList().setValue("PI_CIMTYP", extension);
				profileRoot = SAPUtil.escape(extension);
			}
			setFiltersfromBrowser(func, operationProps);
			conn.executeFunction(func);
			builder.appendSchemaElement(profileRoot, null);
			builder.appendComplexType(null);
			builder.appendSequence();
			builder.appendSchemaElement("IDOC", null);
			builder.appendComplexType(null);
			builder.appendSequence();
			builder.mark(TOP_LEVEL_MARK);
			builder.appendImported((Node) SAPIDocSchemaBuilder.getEDIDC40SchemaElement());
			builder.toMark(TOP_LEVEL_MARK);
			builder.toParent();
			builder.appendSchemaAttribute("BEGIN", "xs:string");
			builder.setUseRequired();
			builder.toMark(TOP_LEVEL_MARK);
			HashMap<String, Object> complexTypes = new HashMap<>();
			JCoTable segFields = func.getTableParameterList().getTable("PT_FIELDS");
			if (segFields != null && segFields.getNumRows() > 0) {
				segFields.firstRow();
				do {
					Object ctMark;
					String complexType = segFields.getString("SEGMENTTYP");
					if ((ctMark = complexTypes.get(complexType)) == null) {
						ctMark = new Object();
						complexTypes.put(complexType, ctMark);
						builder.toRoot();
						builder.appendComplexType(SAPUtil.escape(complexType));
						builder.appendSequence();
						builder.mark(ctMark);
						builder.toParent();
						builder.appendSchemaAttribute("SEGMENT", "xs:string");
						builder.setUseRequired();
						builder.toMark(ctMark);
					} else {
						builder.toMark(ctMark);
					}
					int length = Integer.parseInt(segFields.getString("EXTLEN"));
					SAPSchemaBuilder.appendField(builder, 29, segFields.getString("FIELDNAME"), length, "0", "1",
							segFields.getString("DESCRP"));
					builder.mark(ctMark);
				} while (segFields.nextRow());
			}
			LinkedHashMap<String, Segment> segmentsMap = new LinkedHashMap<>();
			JCoTable segments = func.getTableParameterList().getTable("PT_SEGMENTS");
			if (segments != null && segments.getNumRows() > 0) {
				segments.firstRow();
				do {
					segmentsMap.put(segments.getString("NR"), new Segment(segments));
				} while (segments.nextRow());
			}
			for (Segment seg : segmentsMap.values()) {
				Segment parent = segmentsMap.get(seg.getParentId());
				if (parent == null) {
					builder.toMark(TOP_LEVEL_MARK);
				} else {
					builder.toMark(complexTypes.get(parent.getName()));
				}
				String cleanedSegName = SAPUtil.escape(seg.getName());
				builder.appendSchemaElement(cleanedSegName, cleanedSegName);
				builder.setOccursAttributes(seg.getMinOccurs(), seg.getMaxOccurs());
				SAPSchemaBuilder.appendDocumentation(builder, seg.getDocumentation());
			}
			return builder.getDocument();
		} catch (JCoException  e) {
			logger.log(Level.WARNING, "JCoException occured, Unable to find IDoc base type: {0} , extension: {1}  " , new String[] { functionName , StringUtil.trimToEmpty((String) extension)});
			throw new ConnectorException("Unable to find IDoc base type: " + functionName + ", extension: "+ StringUtil.trimToEmpty((String) extension));
		} catch (Exception e) {
			logger.log(Level.WARNING, "General exception occured", (Throwable) e);
			throw new ConnectorException("Unable to find IDoc base type: General exception occure");
		} finally {
			IOUtil.closeQuietly(conn);
		}
		
	}

	/**
     * This method sets Filters from Browser
     * @param func
     * @param operationProps
     * 
     */
	private void setFiltersfromBrowser(JCoFunction func, PropertyMap operationProps) {

		String segmentRelease = operationProps.getProperty("segmentRelease");
		String applicationRelease = operationProps.getProperty("applicationRelease");

		// Impl for Segment Release
		if (!StringUtil.isEmpty(segmentRelease)) {
			func.getImportParameterList().setValue("PI_RELEASE", segmentRelease);

		}
		// Impl for Segment Application Release
		if (!StringUtil.isEmpty(applicationRelease)) {
			func.getImportParameterList().setValue("PI_APPREL", applicationRelease);

		}

	}

	/**
     * This method returns the elements from EDI DC40 Schema, it reads a xml file return the element object.
     * 
     * @return {@link Element}
     * 
     */
	private static Element getEDIDC40SchemaElement() {
		InputStream schema = null;
		try {
			schema = SAPBAPISchemaBuilder.class.getResourceAsStream(EDI_DC40_XSD);
			return DOMUtil.getFirstElementChild(
					(Node) XMLUtil.parseXML((schema) )
							.getDocumentElement());
		} catch (Exception e) {
			throw new ConnectorException("Unable to import EDI_DC40 schema", (Throwable) e);
		} finally {
			IOUtil.closeQuietly(schema);
		}
	}

	private static class Segment {
		private String name;
		private String parentId;
		private String minOccurs;
		private String maxOccurs;
		private String documentation;

		public Segment(JCoTable table) {
			this.name = table.getString("SEGMENTTYP");
			this.parentId = table.getString("PARPNO");
			this.documentation = table.getString("DESCRP");
			this.minOccurs = StringUtil.isEmpty(table.getString("GRP_MUSTFL"))
					|| StringUtil.isEmpty(table.getString("MUSTFL")) ? "0" : "1";
			long tempMaxOccurs = Math.max(NumberUtil.toLong(table.getString("OCCMAX"), 0L),
					NumberUtil.toLong(table.getString("GRP_OCCMAX"), 0L));
			this.maxOccurs = tempMaxOccurs == 0L ? "0" : (tempMaxOccurs == 1L ? "1" : "unbounded");
		}

		public String getName() {
			return this.name;
		}

		public String getParentId() {
			return this.parentId;
		}

		public String getMinOccurs() {
			return this.minOccurs;
		}

		public String getMaxOccurs() {
			return this.maxOccurs;
		}

		public String getDocumentation() {
			return this.documentation;
		}
	}
	
	/**
	 * This method will validate the given IDoc type.
	 * @param conn
	 * @param functionName
	 * @param operationProps
	 */
	public void validateIDocType(SAPJcoConnection conn, String functionName, PropertyMap operationProps) {
		String extension = operationProps.getProperty("extension");
		try {
			JCoFunction func = conn.getFunction("IDOCTYPE_READ_COMPLETE");
			func.getImportParameterList().setValue("PI_IDOCTYP", functionName);
			if (!StringUtil.isEmpty(extension)) {
				func.getImportParameterList().setValue("PI_CIMTYP", extension);
			}
			setFiltersfromBrowser(func, operationProps);
			conn.executeFunction(func);
		}catch(JCoException e) {
			throw new ConnectorException("Unable to find IDoc base type: " + functionName + ", extension: "+ StringUtil.trimToEmpty(extension) + ", details : " + e.getMessage());
		}
	}

}
