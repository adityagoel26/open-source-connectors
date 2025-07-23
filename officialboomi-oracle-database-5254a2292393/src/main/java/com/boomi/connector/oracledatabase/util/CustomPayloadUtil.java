// Copyright (c) 2020 Boomi, LP.
package com.boomi.connector.oracledatabase.util;

import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.*;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import com.boomi.connector.api.BasePayload;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ExtendedPayload;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import oracle.sql.ArrayDescriptor;
import oracle.sql.Datum;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;

/**
 * The Class CustomPayloadUtil.
 *
 */
public class CustomPayloadUtil extends BasePayload implements ExtendedPayload {

	/** The Constant JSON_FACTORY. */
	private static final JsonFactory JSON_FACTORY = new JsonFactory();

	/** The rs. */
	private ResultSet rs;
	
	
	/** The generator. */
	JsonGenerator generator = null;
	

	/** The batchCount. */
	private long batchCount = 0;

	/** The con. */
	Connection con;

	/**
	 * Creates a new instance. Closing the payload will close the Resultset
	 *
	 * @param resultset the resultset
	 */

	public CustomPayloadUtil(ResultSet resultset) {
		this.rs = resultset;
	}

	/**
	 * Instantiates a new custom payload util with Connection Object.
	 *
	 * @param rs  the rs
	 * @param con the con
	 */
	public CustomPayloadUtil(ResultSet rs, Connection con) {
		this.rs = rs;
		this.con = con;
	}
	
	/**
	 * Instantiates a new custom payload util with Connection Object.
	 *
	 * @param rs  the rs
	 * @param con the con
	 * @param batchCount the batchCount
	 */
	public CustomPayloadUtil(ResultSet resultset, Connection con, long batchCount) {
		this.rs = resultset;
		this.con = con;
		this.batchCount = batchCount;
	}
	/**
	 * This method will write the resultset in Json using JsonGenerator to Output
	 * stream.
	 *
	 * @param out OutputStream
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public void writeTo(OutputStream out) throws IOException {
		try {
			if (batchCount > 0) {
				batchedPayload(batchCount, out);
			} else {
				generator = JSON_FACTORY.createGenerator(out);
				nonBatchedPayload();
			}
		} finally {
			generator.close();
		}
	}
	
	/**
	 * 
	 * @param batchCount
	 * @param out
	 * @throws IOException
	 */
	private void batchedPayload(long batchCount, OutputStream out) throws IOException {
		generator = JSON_FACTORY.createGenerator(out);
		try {			
			generator.writeStartArray();

			for (long i = 0; i < batchCount; i++)  {
				if(rs.getRow()!=0)
				{							
					nonBatchedPayload();
					
					if(i+1<batchCount)
					{					
						rs.next();
					}
				}				
			}			
			generator.writeEndArray();
		generator.flush();
		}catch (SQLException e) {
			throw new ConnectorException(e.getMessage());
		}finally {
			generator.close();
		}
	}
	
	
	/**
	 * This method will write the resultset in Json using JsonGenerator to Output
	 * stream.
	 *
	 * @param out OutputStream
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void nonBatchedPayload() throws IOException {
		try {
			generator.writeStartObject();

			for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
				switch (rs.getMetaData().getColumnType(i)) {
				case 4:
				case 5:
				case -6:
					int value = rs.getInt(rs.getMetaData().getColumnLabel(i));
					generator.writeNumberField(rs.getMetaData().getColumnLabel(i), value);
					generator.flush();
					break;
				case 12:
				case 91:
				case 92:
				case -1:
				case 2005:
				case 93:
				case -9:
				case 1:
				case -15:
				case -16:
					String varchar = rs.getString(rs.getMetaData().getColumnLabel(i));
					generator.writeStringField(rs.getMetaData().getColumnLabel(i), varchar);
					generator.flush();
					break;
				case 16:
				case -7:
					boolean flag = rs.getBoolean(rs.getMetaData().getColumnLabel(i));
					generator.writeBooleanField(rs.getMetaData().getColumnLabel(i), flag);
					generator.flush();
					break;
				case -10:
					this.processRefCursors(i);
					break;
				case 2004:
				case -4:
				case -2:
				case -3:
					if (rs.getBytes(rs.getMetaData().getColumnLabel(i)) != null) {
						generator.writeStringField(rs.getMetaData().getColumnLabel(i),
								new String(rs.getBytes(rs.getMetaData().getColumnLabel(i))));
					}
					generator.flush();
					break;
				case 3:
				case 8:
					double dbValue = rs.getDouble(rs.getMetaData().getColumnLabel(i));
					generator.writeNumberField(rs.getMetaData().getColumnLabel(i), dbValue);
					generator.flush();
					break;
				case 6:
				case 7:
					float fValue = rs.getFloat(rs.getMetaData().getColumnLabel(i));
					generator.writeNumberField(rs.getMetaData().getColumnLabel(i), fValue);
					generator.flush();
					break;
				case -5:
					long lValue = rs.getLong(rs.getMetaData().getColumnLabel(i));
					generator.writeNumberField(rs.getMetaData().getColumnLabel(i), lValue);
					generator.flush();
					break;
				case 2:
					writeNumberField(i);
					break;
				case 2003:
					if(rs.getObject(i) == null) {
						generator.writeNullField(rs.getMetaData().getColumnLabel(i));
					}
					else {
					this.writeArrayField(i);
					}
					break;
				case 2002:
					if(rs.getObject(i) == null) {
						generator.writeNullField(rs.getMetaData().getColumnLabel(i));
					}
					else {
					this.writeObjectValues(i);
					}
					break;
				default:
					break;
				}

			}
			generator.writeEndObject();
			generator.flush();
		} catch (SQLException | IOException e) {
			throw new ConnectException(e.getMessage());
		} 
	}

	/**
	 * This method is used to writes a NUMBER field to output
	 *
	 * @param index Index of the column in the result set
	 * @throws SQLException Throws SQL exception
	 * @throws IOException  Throws IO exception
	 */
	private void writeNumberField(int index) throws SQLException, IOException {
		BigDecimal value = rs.getBigDecimal(rs.getMetaData().getColumnLabel(index));
		generator.writeNumberField(rs.getMetaData().getColumnLabel(index), value);
		generator.flush();
	}
	
	/**
	 * This method will process OUT params of type REFCURSORS. This method will add
	 * the values from the refcursors to the JsonGenerator and flush each column
	 * value content to the temporary OutputStream.
	 *
	 * @param generator the generator
	 * @param csmt      the csmt
	 * @param i         the i
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private void processRefCursors(int i)
			throws SQLException, IOException {
			try (ResultSet set =(ResultSet) (rs.getObject(i))) {
			generator.writeArrayFieldStart(rs.getMetaData().getColumnName(i));
			while (set.next()) {
				generator.writeStartObject();
				for (int l = 1; l <= set.getMetaData().getColumnCount(); l++) {
					switch (set.getMetaData().getColumnType(l)) {
					case Types.INTEGER:
					case Types.NUMERIC:
					case Types.TINYINT:
					case Types.SMALLINT:
						generator.writeNumberField(set.getMetaData().getColumnName(l),
								set.getInt(set.getMetaData().getColumnName(l)));
						break;
					case Types.VARCHAR:
					case Types.CLOB:
					case Types.DATE:
					case Types.TIME:
					case Types.CHAR:
					case Types.NCHAR:
					case Types.LONGVARCHAR:
					case Types.LONGNVARCHAR:
					case Types.NVARCHAR:
					case Types.TIMESTAMP:
						generator.writeStringField(set.getMetaData().getColumnName(l),
								set.getString(set.getMetaData().getColumnName(l)));
						break;
					case Types.REAL:
					case Types.FLOAT:
						generator.writeNumberField(set.getMetaData().getColumnName(l),
								set.getFloat(set.getMetaData().getColumnName(l)));
						break;	
					case Types.DECIMAL:
					case Types.DOUBLE:
						generator.writeNumberField(set.getMetaData().getColumnName(l),
								set.getDouble(set.getMetaData().getColumnName(l)));
						break;		
					case Types.BOOLEAN:
						generator.writeBooleanField(set.getMetaData().getColumnName(l),
								set.getBoolean(set.getMetaData().getColumnName(l)));
						break;
					default:
						break;
					}
					generator.flush();
				}
				generator.writeEndObject();
			}
			generator.writeEndArray();
			generator.flush();
		}
	}
	
	
	
	/**
	 * Method for writing array field to Json Generator.
	 *
	 * @param i the i
	 * @throws IOException  Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
	private void writeArrayField(int i) throws IOException, SQLException {
		ArrayDescriptor desc = ArrayDescriptor.createDescriptor(rs.getMetaData().getColumnTypeName(i), SchemaBuilderUtil.getUnwrapConnection(con));
		boolean type = QueryBuilderUtil.checkArrayDataType(desc);
		if(type) {
			generator.writeFieldName(rs.getMetaData().getColumnName(i));
			generator.writeStartObject();
			Array charar = rs.getArray(i);
			if (desc.getBaseName().equalsIgnoreCase(VARCHAR) || desc.getBaseName().equalsIgnoreCase(CHAR)
					|| desc.getBaseName().equalsIgnoreCase(NCHAR) || desc.getBaseName().equalsIgnoreCase(NVARCHAR)
					|| desc.getBaseName().equalsIgnoreCase(LONGVARCHAR)) {
				writeStringType(charar);
			} else if (desc.getBaseName().equalsIgnoreCase(NUMBER) || desc.getBaseName().equalsIgnoreCase(INTEGER)
					|| desc.getBaseName().equalsIgnoreCase(TINYINT) || desc.getBaseName().equalsIgnoreCase(SMALLINT)
					|| desc.getBaseName().equalsIgnoreCase(NUMERIC) || desc.getBaseName().equalsIgnoreCase(DECIMAL)
					|| desc.getBaseName().equalsIgnoreCase(DOUBLE) || desc.getBaseName().equalsIgnoreCase(FLOAT)) {
				writeNumberType(charar);
			} else if (desc.getBaseName().equalsIgnoreCase(DATE) || desc.getBaseName().equalsIgnoreCase(TIME)
					|| desc.getBaseName().equalsIgnoreCase(TIMESTAMP)) {
				writeTimestampType(charar);
			} 
			generator.flush();
			generator.writeEndObject();
		} else {
			if(rs.getObject(i) == null) {
				generator.writeNullField(rs.getMetaData().getColumnLabel(i));
			}else {
			this.writeNestedTableValues(i);
			}

		}

	}

	/**
	 * 
	 * @param charar
	 * @throws SQLException
	 * @throws IOException
	 */
	private void writeTimestampType(Array charar) throws SQLException, IOException {
		Timestamp[] values = (Timestamp[]) charar.getArray();
		for (int j = 1; j <= values.length; j++) {
			if (null != values[j - 1]) {
				generator.writeStringField(ELEMENT + j, values[j - 1].toString());
			} else {
				generator.writeNullField(ELEMENT + j);
			}

		}
	}

	/**
	 * 
	 * @param charar
	 * @throws SQLException
	 * @throws IOException
	 */
	private void writeNumberType(Array charar) throws SQLException, IOException {
		BigDecimal[] values = (BigDecimal[]) charar.getArray();
		for (int j = 1; j <= values.length; j++) {
			if (null != values[j - 1]) {
				generator.writeNumberField(ELEMENT + j, values[j - 1]);
			} else {
				generator.writeNullField(ELEMENT + j);
			}

		}
	}

	/**
	 * 
	 * @param charar
	 * @throws SQLException
	 * @throws IOException
	 */
	private void writeStringType(Array charar) throws SQLException, IOException {
		String[] values = (String[]) charar.getArray();
		for (int j = 1; j <= values.length; j++) {
			if (null != values[j - 1]) {
				generator.writeStringField(OracleDatabaseConstants.ELEMENT + j, values[j - 1]);
			} else {
				generator.writeNullField(OracleDatabaseConstants.ELEMENT + j);
			}

		}
	}
	
	/**
	 * Write nested table values to Json Generator.
	 *
	 * @param i the i
	 * @throws IOException  Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
	private void writeObjectValues(int i) throws IOException, SQLException {
		java.sql.Struct array = (java.sql.Struct)rs.getObject(i);
		Datum[] data = ((oracle.sql.STRUCT)array).getOracleAttributes();
		StructDescriptor structLevel1 = StructDescriptor
				.createDescriptor(rs.getMetaData().getColumnTypeName(i), SchemaBuilderUtil.getUnwrapConnection(con));
		generator.writeArrayFieldStart(rs.getMetaData().getColumnLabel(i));
		generator.writeStartObject();
		int k = 0;
		for (Object element : data) {
				k++;
				if (element == null) {
					generator.writeNullField(structLevel1.getMetaData().getColumnName(k));
				} else if (structLevel1.getMetaData().getColumnTypeName(k).equalsIgnoreCase(TINYINT)||
						structLevel1.getMetaData().getColumnTypeName(k).equalsIgnoreCase(SMALLINT)||
						structLevel1.getMetaData().getColumnTypeName(k).equalsIgnoreCase(NUMERIC)||
						structLevel1.getMetaData().getColumnTypeName(k).equalsIgnoreCase(DECIMAL)||
						structLevel1.getMetaData().getColumnTypeName(k).equalsIgnoreCase(DOUBLE)) {
					generator.writeNumberField(structLevel1.getMetaData().getColumnName(k),((oracle.sql.NUMBER) element).doubleValue());
					generator.flush();
				} else if (structLevel1.getMetaData().getColumnTypeName(k).equalsIgnoreCase(VARCHAR)
						|| structLevel1.getMetaData().getColumnTypeName(k).equalsIgnoreCase(CHAR)
						|| structLevel1.getMetaData().getColumnTypeName(k).equalsIgnoreCase(LONGVARCHAR)
						|| structLevel1.getMetaData().getColumnTypeName(k).equalsIgnoreCase(NCHAR)
						|| structLevel1.getMetaData().getColumnTypeName(k).equalsIgnoreCase(LONGNVARCHAR)
						|| structLevel1.getMetaData().getColumnTypeName(k).equalsIgnoreCase(TIMESTAMP)
						|| structLevel1.getMetaData().getColumnTypeName(k).equalsIgnoreCase(DATE)
						|| structLevel1.getMetaData().getColumnTypeName(k).equalsIgnoreCase(NVARCHAR)
						|| element instanceof String || element instanceof Timestamp) {
					generator.writeStringField(structLevel1.getMetaData().getColumnName(k), element.toString().replace(BACKSLASH, ""));
					generator.flush();
				}
				else if (structLevel1.getMetaData().getColumnTypeName(k).equalsIgnoreCase(REAL)||
						structLevel1.getMetaData().getColumnTypeName(k).equalsIgnoreCase(FLOAT)) {
					generator.writeNumberField(structLevel1.getMetaData().getColumnName(k),((oracle.sql.NUMBER) element).floatValue());
					generator.flush();
				}
				else if (structLevel1.getMetaData().getColumnTypeName(k).equalsIgnoreCase(NUMBER)||
						structLevel1.getMetaData().getColumnTypeName(k).equalsIgnoreCase(INTEGER)||element instanceof oracle.sql.NUMBER) {
					generator.writeNumberField(structLevel1.getMetaData().getColumnName(k),((oracle.sql.NUMBER) element).intValue());
					generator.flush();
				} else if (element instanceof BigDecimal) {
					generator.writeNumberField(structLevel1.getMetaData().getColumnName(k), (BigDecimal) element);
				} else if (element instanceof java.sql.Blob) {
					Blob b = (Blob) element;
					byte[] byteArray = b.getBytes(1, (int) b.length());
					String data1 = new String(byteArray, StandardCharsets.UTF_8);
					generator.writeStringField(structLevel1.getMetaData().getColumnName(k), data1);
					generator.flush();
				} else if (element instanceof oracle.sql.RAW) {
					oracle.sql.RAW b = (oracle.sql.RAW) element;
					byte[] byteArray = b.getBytes();
					String data1 = new String(byteArray, StandardCharsets.UTF_8);
					generator.writeStringField(structLevel1.getMetaData().getColumnName(k),data1);
					generator.flush();
				}
		}
		generator.writeEndObject();
		generator.writeEndArray();
		
	}
	
	

	/**
	 * Write nested table values to Json Generator.
	 *
	 * @param i the i
	 * @throws IOException  Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
	private void writeNestedTableValues(int i) throws IOException, SQLException {

		Object[] data = (Object[]) ((Array) rs.getObject(i)).getArray();
		generator.writeArrayFieldStart(rs.getMetaData().getColumnLabel(i));
		for (Object element : data) {
			STRUCT rowLevel1 = (STRUCT) element;
			StructDescriptor structLevel1 = rowLevel1.getDescriptor();
			generator.writeStartObject();
			int k = 0;
			for (Object attribute : rowLevel1.getAttributes()) {
				k++;
				if (attribute instanceof oracle.sql.NUMBER || attribute instanceof BigDecimal) {
					generator.writeNumberField(structLevel1.getMetaData().getColumnName(k), (BigDecimal) attribute);
				} else if (attribute instanceof String ||attribute instanceof Timestamp) {
					generator.writeStringField(structLevel1.getMetaData().getColumnName(k), attribute.toString().replace(BACKSLASH, ""));
				} else if (attribute instanceof java.sql.Blob) {
					Blob b = (Blob) attribute;
					byte[] byteArray = b.getBytes(1, (int) b.length());
					String data1 = new String(byteArray, StandardCharsets.UTF_8);
					generator.writeStringField(structLevel1.getMetaData().getColumnName(k),data1);
					generator.flush();
				} else if (attribute instanceof oracle.sql.STRUCT) {
					this.processObjectStruct2(k, attribute, structLevel1.getMetaData().getColumnName(k), structLevel1);
				}
				else if (attribute instanceof oracle.sql.ARRAY) {
					writeInnerNestedTableValues(structLevel1, k, attribute);
				}
				else {
					generator.writeObjectField(structLevel1.getMetaData().getColumnName(k), attribute);
				}

			}
			generator.writeEndObject();
		}
		generator.writeEndArray();
	}

	/**
	 * 
	 * @param structLevel1
	 * @param k
	 * @param attribute
	 * @throws SQLException
	 * @throws IOException
	 */
	private void writeInnerNestedTableValues(StructDescriptor structLevel1, int k, Object attribute)
			throws SQLException, IOException {
		ArrayDescriptor desc2 = ArrayDescriptor.createDescriptor(structLevel1.getMetaData().getColumnTypeName(k), SchemaBuilderUtil.getUnwrapConnection(con));
		boolean type = QueryBuilderUtil.checkArrayDataType(desc2);
		if(type) {
			this.arrayElement(desc2, attribute, structLevel1.getMetaData().getColumnName(k));	
		}
		else {
			this.writeNestedTableValues2(StructDescriptor.createDescriptor(desc2.getBaseName(),SchemaBuilderUtil.getUnwrapConnection(con)),attribute,structLevel1.getMetaData().getColumnName(k) );
		}
	}
	
	/**
	 * 
	 * @param i
	 * @param element
	 * @param argument
	 * @param structDescriptor1
	 * @throws SQLException
	 * @throws IOException
	 */
	private void processObjectStruct2(int i, Object element, String argument,
			StructDescriptor structDescriptor1)
			throws SQLException, IOException {
		StructDescriptor structDescriptor = StructDescriptor.createDescriptor(structDescriptor1.getOracleTypeADT().getAttributeType(i), SchemaBuilderUtil.getUnwrapConnection(con));
		java.sql.Struct array = (java.sql.Struct)element;
		Datum[] data = ((oracle.sql.STRUCT)array).getOracleAttributes();
		ResultSetMetaData md = structDescriptor.getMetaData();
		generator.writeArrayFieldStart(argument);
				generator.writeStartObject();
				int k = 0;
				for (Object elements1 : data) {	
					k++;
			if (elements1 == null) {
				generator.writeNullField(md.getColumnName(k));
				generator.flush();
			} 
			else if (TINYINT.equalsIgnoreCase(md.getColumnTypeName(k))
					|| SMALLINT.equalsIgnoreCase(md.getColumnTypeName(k))
					|| NUMERIC.equalsIgnoreCase(md.getColumnTypeName(k))
					|| DECIMAL.equalsIgnoreCase(md.getColumnTypeName(k))
					|| DOUBLE.equalsIgnoreCase(md.getColumnTypeName(k))) {
				generator.writeNumberField(md.getColumnName(k),((oracle.sql.NUMBER) elements1).doubleValue());
				generator.flush();
			}
			else if (VARCHAR.equalsIgnoreCase(md.getColumnTypeName(k)) || CHAR.equalsIgnoreCase(md.getColumnTypeName(k))
					|| LONGVARCHAR.equalsIgnoreCase(md.getColumnTypeName(k))
					|| NCHAR.equalsIgnoreCase(md.getColumnTypeName(k))
					|| LONGNVARCHAR.equalsIgnoreCase(md.getColumnTypeName(k))
					|| TIMESTAMP.equalsIgnoreCase(md.getColumnTypeName(k))
					|| DATE.equalsIgnoreCase(md.getColumnTypeName(k))
					|| NVARCHAR.equalsIgnoreCase(md.getColumnTypeName(k)) || elements1 instanceof String
					|| elements1 instanceof Timestamp) {
				generator.writeStringField(md.getColumnName(k), elements1.toString().replace(BACKSLASH, ""));
				generator.flush();
			}
			else if (REAL.equalsIgnoreCase(md.getColumnTypeName(k))
					|| FLOAT.equalsIgnoreCase(md.getColumnTypeName(k))) {
				generator.writeNumberField(md.getColumnName(k),((oracle.sql.NUMBER) elements1).floatValue());
				generator.flush();
			}
			else if (elements1 instanceof java.math.BigDecimal) {
				generator.writeNumberField(md.getColumnName(k),((BigDecimal) elements1).intValue());
				generator.flush();
			}else if (NUMBER.equalsIgnoreCase(md.getColumnTypeName(k))
					|| INTEGER.equalsIgnoreCase(md.getColumnTypeName(k))
					|| elements1 instanceof oracle.sql.NUMBER) {
				generator.writeNumberField(md.getColumnName(k),((oracle.sql.NUMBER) elements1).intValue());
				generator.flush();
			} 
			else if (elements1 instanceof java.sql.Blob) {
				Blob b = (Blob) elements1;
				byte[] byteArray = b.getBytes(1, (int) b.length());
				String data1 = new String(byteArray, StandardCharsets.UTF_8);
				generator.writeStringField(md.getColumnName(k),data1);
				generator.flush();
			}
			else if (elements1 instanceof oracle.sql.RAW) {
				oracle.sql.RAW b = (oracle.sql.RAW) elements1;
				byte[] byteArray = b.getBytes();
				String data1 = new String(byteArray, StandardCharsets.UTF_8);
				generator.writeStringField(md.getColumnName(k),data1);
				generator.flush();
			}
		}
				generator.writeEndObject();
		generator.writeEndArray();
	}
	
	
	
	/**
	 * 
	 * @param structlevel2
	 * @param attribute2
	 * @param argument
	 * @throws IOException
	 * @throws SQLException
	 */
	private void writeNestedTableValues2(StructDescriptor structlevel2, Object attribute2, String argument) throws IOException, SQLException{
		Object[] array = (Object[]) ((oracle.sql.ARRAY) attribute2).getArray();
		
		generator.writeArrayFieldStart(argument);
		
		ResultSetMetaData md = structlevel2.getMetaData();
		for (int j = 0; j < array.length; j++) {
			STRUCT rowLevel2 = (STRUCT) array[j];
			generator.writeStartObject();
			int k = 0;
			for (Object element1 : rowLevel2.getAttributes()) {
				k++;
				if (element1 == null) {
					generator.writeNullField(md.getColumnName(k));
				} else if (element1 instanceof BigDecimal) {
					generator.writeNumberField(md.getColumnName(k), ((BigDecimal) element1).floatValue());
				} else if (element1 instanceof String || element1 instanceof Timestamp) {
					generator.writeStringField(md.getColumnName(k), element1.toString().replace(BACKSLASH, ""));
				} else if (element1 instanceof java.sql.Blob) {
					Blob b = (Blob) element1;
					byte[] byteArray = b.getBytes(1, (int) b.length());
					String data1 = new String(byteArray, StandardCharsets.UTF_8);
					generator.writeStringField(md.getColumnName(k), data1);
					generator.flush();
				} else if (element1 instanceof oracle.sql.STRUCT) {
					this.processObjectStruct2(k, element1,md.getColumnName(k), structlevel2);
				}
				else if (element1 instanceof Array) {
					writeInnerNestedTableValues2(structlevel2, md, k, element1);
				}
			}
			generator.writeEndObject();
		}
		generator.writeEndArray();
	}

	/**
	 * 
	 * @param structlevel2
	 * @param md
	 * @param k
	 * @param element1
	 * @throws SQLException
	 * @throws IOException
	 */
	private void writeInnerNestedTableValues2(StructDescriptor structlevel2, ResultSetMetaData md, int k,
			Object element1) throws SQLException, IOException {
		ArrayDescriptor arrayLevel3 = ArrayDescriptor
				.createDescriptor(structlevel2.getMetaData().getColumnTypeName(k), SchemaBuilderUtil.getUnwrapConnection(con));
		boolean type = QueryBuilderUtil.checkArrayDataType(arrayLevel3);
		String columnName =  md.getColumnName(k);
		if(type) {
			this.arrayElement(arrayLevel3, element1, structlevel2.getMetaData().getColumnName(k) );						}
		else {
			this.writeNestedTableValues2(StructDescriptor.createDescriptor(arrayLevel3.getBaseName(),SchemaBuilderUtil.getUnwrapConnection(con)), element1, columnName);
		}
	}
	
	
/**
 * 
 * @param arrayLevel
 * @param generator
 * @param element1
 * @param argument
 * @throws SQLException
 * @throws IOException
 */
	private void arrayElement(ArrayDescriptor arrayLevel, Object element1, String argument) throws SQLException, IOException {
		
		boolean type = QueryBuilderUtil.checkArrayDataType(arrayLevel);
		if(type) {
			generator.writeFieldName(argument);
			generator.writeStartObject();
			if (arrayLevel.getBaseName().equalsIgnoreCase(VARCHAR) || arrayLevel.getBaseName().equalsIgnoreCase(CHAR)
					|| arrayLevel.getBaseName().equalsIgnoreCase(NCHAR) || arrayLevel.getBaseName().equalsIgnoreCase(NVARCHAR)
					|| arrayLevel.getBaseName().equalsIgnoreCase(LONGVARCHAR)) {
				writeVarcharArrayField(element1);
			} else if (arrayLevel.getBaseName().equalsIgnoreCase(NUMBER)
					|| arrayLevel.getBaseName().equalsIgnoreCase(INTEGER)
					|| arrayLevel.getBaseName().equalsIgnoreCase(TINYINT)
					|| arrayLevel.getBaseName().equalsIgnoreCase(SMALLINT)
					|| arrayLevel.getBaseName().equalsIgnoreCase(NUMERIC)
					|| arrayLevel.getBaseName().equalsIgnoreCase(DECIMAL)
					|| arrayLevel.getBaseName().equalsIgnoreCase(DOUBLE)
					|| arrayLevel.getBaseName().equalsIgnoreCase(FLOAT)) {
				writeStringArrayField(element1);
			} else if (arrayLevel.getBaseName().equalsIgnoreCase(DATE) || arrayLevel.getBaseName().equalsIgnoreCase(TIME)
					|| arrayLevel.getBaseName().equalsIgnoreCase(TIMESTAMP)) {
				writeTimestampArrayField(element1);
			} 
			generator.flush();
			generator.writeEndObject();
		} else {
			if(element1 == null) {
				generator.writeNullField((String)element1);
			}else {
				this.writeNestedTableValues2(StructDescriptor.createDescriptor(arrayLevel.getBaseName(),SchemaBuilderUtil.getUnwrapConnection(con)),element1,argument);
			}

		}

	
	}

	/**
	 * 
	 * @param element1
	 * @throws SQLException
	 * @throws IOException
	 */
private void writeVarcharArrayField(Object element1) throws SQLException, IOException {
	String[] values = (String[]) ((oracle.sql.ARRAY) element1).getArray();
	for (int j = 1; j <= values.length; j++) {
		int k = j - 1;
		if (null != values[k]) {
			generator.writeStringField(OracleDatabaseConstants.ELEMENT + j, values[k]);
		} else {
			generator.writeNullField(OracleDatabaseConstants.ELEMENT + j);
		}

	}
}

	/**
	 * 
	 * @param element1
	 * @throws SQLException
	 * @throws IOException
	 */
private void writeTimestampArrayField(Object element1) throws SQLException, IOException {
	Timestamp[] values = (Timestamp[]) ((oracle.sql.ARRAY) element1).getArray();
	for (int j = 1; j <= values.length; j++) {
		int k = j - 1;
		if (null != values[k]) {
			generator.writeStringField(ELEMENT + j, values[k].toString());
		} else {
			generator.writeNullField(ELEMENT + j);
		}

	}
}

/**
 * 
 * @param element1
 * @throws SQLException
 * @throws IOException
 */
private void writeStringArrayField(Object element1) throws SQLException, IOException {
	BigDecimal[] values = (BigDecimal[]) ((oracle.sql.ARRAY) element1).getArray();
	for (int j = 1; j <= values.length; j++) {
		int k = j - 1;
		if (null != values[k]) {
			generator.writeNumberField(ELEMENT + j, values[k]);
		} else {
			generator.writeNullField(ELEMENT + j);
		}

	}
}
	
	/**
	 * Close.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public void close() throws IOException {
		try {
			if (!generator.isClosed()) {
				generator.close();
			}

		} catch (Exception e) {
			throw new ConnectorException(e.getMessage());
		}
	}

}
