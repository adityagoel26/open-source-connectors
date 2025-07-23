// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.databaseconnector.util;

import com.boomi.connector.api.BasePayload;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.PayloadMetadata;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.ConnectException;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.JSON;

/**
 * @author swastik.vn
 **/
public class CustomPayloadUtil extends BasePayload {

	/** The Constant JSON_FACTORY. */
	private static final JsonFactory JSON_FACTORY = new JsonFactory();

	/** The rs. */
	private ResultSet rs;

	/** The generator. */
	JsonGenerator generator = null;
	
	/** The batchCount. */
	private long batchCount = 0;

	private PayloadMetadata _metadata;

	/**
	 * Creates a new instance. Closing the payload will close the Resultset
	 *
	 * @param resultset the resultset
	 */

	public CustomPayloadUtil(ResultSet resultset) {
		this.rs = resultset;
		
	}
	
	public CustomPayloadUtil(ResultSet resultset, long batchCount) {
		this.rs = resultset;
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
				nonBatchedPayload(out);
			}
		} finally {
			generator.close();
		}
	}

	/**
	 * Get {@link PayloadMetadata}.
	 *
	 * @return
	 */
	@Override
	public PayloadMetadata getMetadata() {
		return _metadata;
	}

	/**
	 * Set {@link PayloadMetadata}
	 * @param metadata
	 */
	public void setMetadata(PayloadMetadata metadata) {
		_metadata = metadata;
	}

	private void batchedPayload(long batchCount, OutputStream out) throws IOException {
		generator = JSON_FACTORY.createGenerator(out);
		try {			
			generator.writeStartArray();

			for (long i = 0; i < batchCount; i++)  {
				if(rs.getRow()!=0)
				{							
					nonBatchedPayload(out);
					
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
		
		private void nonBatchedPayload(OutputStream out) throws IOException {
		try {
			generator.writeStartObject();

			for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
				if (rs.getMetaData().getColumnType(i) == 4 || rs.getMetaData().getColumnType(i) == 5 
					|| rs.getMetaData().getColumnType(i) == -6) {
					int value = rs.getInt(rs.getMetaData().getColumnLabel(i));
					if (rs.wasNull()) {
						generator.writeNullField(rs.getMetaData().getColumnLabel(i));
					} else {
						generator.writeNumberField(rs.getMetaData().getColumnLabel(i), value);
					}
					generator.flush();
				}
				else if (rs.getMetaData().getColumnType(i) == 12 || rs.getMetaData().getColumnType(i) == 91
						|| rs.getMetaData().getColumnType(i) == 92 || rs.getMetaData().getColumnType(i) == -1 
						|| rs.getMetaData().getColumnType(i) == 2005 || rs.getMetaData().getColumnType(i) == 93 
						|| rs.getMetaData().getColumnType(i) == -9 || rs.getMetaData().getColumnTypeName(i).equalsIgnoreCase(JSON)
						||  rs.getMetaData().getColumnType(i) == 1 ||  rs.getMetaData().getColumnType(i) == -15
						|| rs.getMetaData().getColumnType(i) == -16 || rs.getMetaData().getColumnType(i) == -8) {
					String varchar = rs.getString(rs.getMetaData().getColumnLabel(i));
					generator.writeStringField(rs.getMetaData().getColumnLabel(i), varchar);
					generator.flush();
				} else if (rs.getMetaData().getColumnType(i) == 16 || rs.getMetaData().getColumnType(i) == -7) {
					boolean flag = rs.getBoolean(rs.getMetaData().getColumnLabel(i));
					if (rs.wasNull()) {
						generator.writeNullField(rs.getMetaData().getColumnLabel(i));
					} else {
						generator.writeBooleanField(rs.getMetaData().getColumnLabel(i), flag);
					}
					generator.flush();
				}else if(rs.getMetaData().getColumnType(i) == 2004 || rs.getMetaData().getColumnType(i) == -4
						|| rs.getMetaData().getColumnType(i) == -2 || rs.getMetaData().getColumnType(i) == -3) {
					if(rs.getBytes(rs.getMetaData().getColumnLabel(i))!=null) {
						generator.writeStringField(rs.getMetaData().getColumnLabel(i), new String(rs.getBytes(rs.getMetaData().getColumnLabel(i))));
					}
					generator.flush();
				}
				else if(rs.getMetaData().getColumnType(i) == 3 || rs.getMetaData().getColumnType(i) == 8) {
					double value = rs.getDouble(rs.getMetaData().getColumnLabel(i));
					if (rs.wasNull()) {
						generator.writeNullField(rs.getMetaData().getColumnLabel(i));
					} else {
						generator.writeNumberField(rs.getMetaData().getColumnLabel(i), value);
					}
					generator.flush();
				}
				else if(rs.getMetaData().getColumnType(i) == 6 || rs.getMetaData().getColumnType(i) == 7) {
					float value = rs.getFloat(rs.getMetaData().getColumnLabel(i));
					if (rs.wasNull()) {
						generator.writeNullField(rs.getMetaData().getColumnLabel(i));
					} else {
						generator.writeNumberField(rs.getMetaData().getColumnLabel(i), value);
					}
					generator.flush();
				}
				else if(rs.getMetaData().getColumnType(i) == -5) {
					long value = rs.getLong(rs.getMetaData().getColumnLabel(i));
					if (rs.wasNull()) {
						generator.writeNullField(rs.getMetaData().getColumnLabel(i));
					} else {
						generator.writeNumberField(rs.getMetaData().getColumnLabel(i), value);
					}
					generator.flush();
				}
				else if(rs.getMetaData().getColumnType(i) == 2) {
					BigDecimal value = rs.getBigDecimal(rs.getMetaData().getColumnLabel(i));
					if (rs.wasNull()) {
						generator.writeNullField(rs.getMetaData().getColumnLabel(i));
					} else {
						generator.writeNumberField(rs.getMetaData().getColumnLabel(i), value);
					}
					}
					generator.flush();
				}
			generator.writeEndObject();
			generator.flush();
		} catch (SQLException e) {
			throw new ConnectException(e.getMessage());
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
