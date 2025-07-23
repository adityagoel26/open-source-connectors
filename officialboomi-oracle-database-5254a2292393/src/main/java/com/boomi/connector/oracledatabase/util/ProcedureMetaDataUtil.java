// Copyright (c) 2020 Boomi, LP.
package com.boomi.connector.oracledatabase.util;

import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.boomi.connector.api.ConnectorException;

/**
 * The Class ProcedureMetaDataUtil.
 *
 * @author swastik.vn
 */
public class ProcedureMetaDataUtil {
	
	/** The out params. */
	private List<String> outParams = new ArrayList<>();
	
	/** The in params. */
	private List<String> inParams = new ArrayList<>();
	
	/** The params. */
	private List<String> params = new ArrayList<>();
	
	/** The data type. */
	private Map<String, Integer> dataType = new HashMap<>();
	

	/**
	 * Instantiates a new procedure meta data util.
	 *
	 * @param con the con
	 * @param procedureName the procedure name
	 * @param procedureNameWithPackage
	 * @throws SQLException 
	 */
	public ProcedureMetaDataUtil(Connection con, String procedureName,String procedureNameWithPackage ) throws SQLException { 
		getProcedureMetadata(con, procedureName, procedureNameWithPackage);
	}

	/**
	 * This method will get the Input Parameters along with DataType required for
	 * the procedure call.
	 *
	 * @param con the con
	 * @param objectTypeId the object type id
	 * @return the procedure metadata
	 * @throws SQLException 
	 */
	private void getProcedureMetadata(Connection con, String objectTypeId, String procedureNameWithPackage) throws SQLException {
		String procedure = SchemaBuilderUtil.getProcedureName(objectTypeId);
		String packageName = SchemaBuilderUtil.getProcedurePackageName(procedureNameWithPackage);
		
		String schema = null;
		schema = checkForSchemaName(con, packageName);
		try (ResultSet rs = con.getMetaData().getProcedureColumns(schema, con.getSchema(), procedure, null)) {
			while (rs.next()) {
				if (rs.getString(TYPE_NAME).equals("REF CURSOR")) {
					dataType.put(rs.getString(COLUMN_NAME), 2012);
				} else if (rs.getString(TYPE_NAME).equals(TABLE)) {
					dataType.put(rs.getString(COLUMN_NAME), 2010);
				}
				else if (rs.getString(TYPE_NAME).equalsIgnoreCase(DATE)) {
					dataType.put(rs.getString(COLUMN_NAME), 91);
				}
				else if(rs.getString(COLUMN_NAME) != null){
					dataType.put(rs.getString(COLUMN_NAME), Integer.valueOf(rs.getString(6)));
				}
				checkForColumnType(rs);
			}
		} catch (SQLException e) {
			throw new ConnectorException(e.getMessage());
		}
	}

	private void checkForColumnType(ResultSet rs) throws SQLException {
		if (rs.getString(COLUMN_NAME) != null && !params.contains(rs.getString(COLUMN_NAME)))
			params.add(rs.getString(COLUMN_NAME));
		if ((rs.getShort(5) == 1 || rs.getShort(5) == 2) && !inParams.contains(rs.getString(4)))
			inParams.add(rs.getString(4));
		if ((rs.getShort(5) == 2 || rs.getShort(5) == 4) && !outParams.contains(rs.getString(4)))
			outParams.add(rs.getString(4));
	}

	private String checkForSchemaName(Connection con, String packageName) throws SQLException {
		String schema;
		if(packageName!=null) {
			schema = packageName;
		}
		else {
			schema = con.getCatalog();
		}
		return schema;
	}
	
	/**
	 * Gets the out params.
	 *
	 * @return the out params
	 */
	public List<String> getOutParams() {
		return outParams;
	}

	/**
	 * Gets the in params.
	 *
	 * @return the in params
	 */
	public List<String> getInParams() {
		return inParams;
	}

	/**
	 * Gets the params.
	 *
	 * @return the params
	 */
	public List<String> getParams() {
		return params;
	}

	/**
	 * Gets the data type.
	 *
	 * @return the data type
	 */
	public Map<String, Integer> getDataType() {
		return dataType;
	}

}
