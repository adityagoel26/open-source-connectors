// Copyright (c) 2021 Boomi, LP.
package com.boomi.connector.oracledatabase.util;

import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.*;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationType;
import oracle.sql.ArrayDescriptor;
import oracle.sql.StructDescriptor;

/**
 * The Class ImportableUtil.
 * 
 * @author swastik.vn
 * 
 */
public class ImportableUtil {

	/** The con. */
	Connection con;

	/** The object type id. */
	String objectTypeId;

	/**
	 * Instantiates a new importable util.
	 *
	 * @param con       the con
	 * @param tableName the table name
	 */
	public ImportableUtil(Connection con, String tableName) {
		this.con = con;
		this.objectTypeId = tableName;
	}

	/**
	 * Builds the importable fields.
	 *
	 * @param opsType       the ops type
	 * @param customOpsType the custom ops type
	 * @param upsertType    the upsert type
	 * @return the string
	 * @throws SQLException the SQL exception
	 */
	public String buildImportableFields(OperationType opsType, String customOpsType, String upsertType)
			throws SQLException {
		StringBuilder query = new StringBuilder("");
		if (OperationType.CREATE.equals(opsType) || (OperationType.UPSERT.equals(opsType)
				&& OracleDatabaseConstants.STANDARD_UPSERT.equalsIgnoreCase(upsertType))) {
			query = QueryBuilderUtil.buildInitialPstmntInsert(con, objectTypeId);
		} else if (opsType.equals(OperationType.UPDATE)) {
			query = nestedUpdate(upsertType, "");
			query.append(OracleDatabaseConstants.WHERE.toUpperCase());
			buildImportableUpdate(query, true, "");
		} else if (opsType.equals(OperationType.UPSERT)
				&& OracleDatabaseConstants.STANDARD_UPSERT.equalsIgnoreCase(upsertType)) {
				query = QueryBuilderUtil.buildInitialPstmntInsert(con, objectTypeId);

		} else if (DELETE.equalsIgnoreCase(customOpsType)) {
			query.append("DELETE FROM ").append(objectTypeId).append(OracleDatabaseConstants.WHERE.toUpperCase());
			buildImportableUpdate(query, true, customOpsType);
		} else if (GET.equalsIgnoreCase(customOpsType)) {
			query.append("SELECT * FROM ").append(objectTypeId).append(OracleDatabaseConstants.WHERE.toUpperCase());
			buildImportableUpdate(query, true, customOpsType);
		}
		return query.toString();
	}

	/**
	 * Builds the importable update.
	 *
	 * @param query         the query
	 * @param and           the and
	 * @param customOpsType the custom ops type
	 * @throws SQLException the SQL exception
	 */
	public void buildImportableUpdate(StringBuilder query, boolean and, String customOpsType) throws SQLException {
		try (ResultSet set = con.getMetaData().getColumns(con.getCatalog(), con.getSchema(), objectTypeId, null)) {
			boolean suffix = false;
			while (set.next()) {
				if (suffix) {
					appendSuffix(query, and);
				}
				query.append(set.getString(COLUMN_NAME));
				if (customOpsType.equals("GET")) {
					query.append("=$").append(set.getString(COLUMN_NAME));
				} else {
					query.append("=?");
				}
				suffix = true;
			}
		}
	}

	/**
	 * Builds importable query for nested update
	 *
	 * @param upsertType the upsert type
	 * @param pk         the pk
	 * @return the string builder
	 * @throws SQLException the SQL exception
	 */
	public StringBuilder nestedUpdate(String upsertType, String pk) throws SQLException {

		StringBuilder query = new StringBuilder("UPDATE ");
		if (!OracleDatabaseConstants.STANDARD_UPSERT.equals(upsertType)) {
			query.append(objectTypeId).append(' ');
		}
		query.append("SET ");
		DatabaseMetaData md = con.getMetaData();
		boolean comma = false;
		try (ResultSet rs = md.getColumns(con.getCatalog(), con.getSchema(), objectTypeId, null)) {
			while (rs.next()) {
				if (comma) {
					query.append(COMMA);
				}
				if (rs.getString(DATA_TYPE).equals("2003")) {
					query.append(rs.getString(COLUMN_NAME)).append('=');
					ArrayDescriptor arraydes1 = ArrayDescriptor
							.createDescriptor(con.getMetaData().getUserName() + DOT + rs.getString(TYPE_NAME), SchemaBuilderUtil.getUnwrapConnection(con));
					boolean type = QueryBuilderUtil.checkArrayDataType(arraydes1);
					if(type){
						query.append(rs.getString(TYPE_NAME)).append('=');
						query.append('?');
					} else {
						innerNestedUpdate(query, rs, arraydes1);
					}
					comma = true;
				} else if (!rs.getString(COLUMN_NAME).equalsIgnoreCase(pk)) {
					query.append(rs.getString(COLUMN_NAME)).append("=?");
					comma = true;
				}
				
			}
		} catch (Exception e) {
			throw new ConnectorException("Error while building Importable fields for " + objectTypeId);
		}

		return query;
	}

	
	/**
	 * 
	 * @param query
	 * @param rs
	 * @param arraydes1
	 * @throws SQLException
	 */
	private void innerNestedUpdate(StringBuilder query, ResultSet rs, ArrayDescriptor arraydes1) throws SQLException {
		query.append(rs.getString(TYPE_NAME)).append('(')
				.append(arraydes1.getBaseName().replaceAll(con.getMetaData().getUserName() + DOT, ""))
				.append('(');
		StructDescriptor struct1 = StructDescriptor.createDescriptor(arraydes1.getBaseName(), SchemaBuilderUtil.getUnwrapConnection(con));
		ResultSetMetaData rsmd1 = struct1.getMetaData();
		for (int j = 1; j <= rsmd1.getColumnCount(); j++) {
			if (rsmd1.getColumnType(j) == 2003) {
				query.append(rsmd1.getColumnTypeName(j)
						.replaceAll(con.getMetaData().getUserName() + DOT, "")).append('(');
				ArrayDescriptor arraydes2 = ArrayDescriptor.createDescriptor(rsmd1.getColumnTypeName(j),
						SchemaBuilderUtil.getUnwrapConnection(con));
				boolean type2 = QueryBuilderUtil.checkArrayDataType(arraydes2);
				if(type2) {
					query.append(PARAM);
				}
				
				else {
					finalNestedUpdate(query, arraydes2);
				}
			}else {
				query.append(PARAM);
			}

		}
		query.append("))");
	}

	/**
	 * 
	 * @param query
	 * @param arraydes2
	 * @throws SQLException
	 */
	private void finalNestedUpdate(StringBuilder query, ArrayDescriptor arraydes2) throws SQLException {
		query.append(arraydes2.getBaseName()
				.replaceAll(con.getMetaData().getUserName() + DOT, "")).append('(');
		StructDescriptor struct2 = StructDescriptor
				.createDescriptor(arraydes2.getBaseName(), SchemaBuilderUtil.getUnwrapConnection(con));
		ResultSetMetaData rsmd2 = struct2.getMetaData();
		for (int k = 1; k <= rsmd2.getColumnCount(); k++) {
			if (rsmd2.getColumnType(k) == 2003) {
				ArrayDescriptor arraydes3 = ArrayDescriptor
						.createDescriptor(rsmd2.getColumnTypeName(k), SchemaBuilderUtil.getUnwrapConnection(con));
				boolean type3 = QueryBuilderUtil.checkArrayDataType(arraydes3);
				if (type3) {
					query.append(PARAM);
				} else {
					throw new ConnectorException("Nested table level exhuasted!!!");
				}
			} else {
				query.append(PARAM);
			}
		}
		query.deleteCharAt(query.length() - 1);
		query.append("))");
	}

	/**
	 * Append suffix.
	 *
	 * @param query the query
	 * @param and   the and
	 */
	private static void appendSuffix(StringBuilder query, boolean and) {
		if (and) {
			query.append(" AND ");
		} else {
			query.append(", ");
		}
	}

}
