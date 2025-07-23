// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sapjco;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.dbcp.BasicDataSource;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.sapjco.util.PayloadDetails;
import com.boomi.connector.sapjco.util.SAPJcoConstants;
import com.boomi.util.DBUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.StringUtil;
import com.sap.conn.jco.server.JCoServerContext;
import com.sap.conn.jco.server.JCoServerTIDHandler;

/**
 * @author kishore.pulluru
 *
 */
public class SAPTIDHandler implements JCoServerTIDHandler {

	private static final Logger logger = Logger.getLogger(SAPTIDHandler.class.getName());

	private static final String CHECKED = "CHECKED";
	private static final String CONFIRMED = "CONFIRMED";
	private static final String COMMITED = "COMMITED";
	private static final String ROLLBACK = "ROLLBACK";

	private static final String TID_MGMT = "NEW_TID_MGMT";
	private static final String IDOC_PAYLOAD = "IDOC_PAYLOAD";
	private static final String EXISTS = "EXISTS_CHECK";
	private static final String DONT_DELETE = "DONT_DELETE";
	private static final String CREATE_SQL = "CREATE TABLE " + TID_MGMT
			+ " ( TID varchar(255), PROG_ID varchar(255), STATUS varchar(255), PREVIOUS_STATUS varchar(255), CONSTRAINT pk_TID PRIMARY KEY (TID,PROG_ID) )";
	private static final String INSERT_SQL = "INSERT INTO " + TID_MGMT + " (TID, PROG_ID, STATUS) VALUES (?,?,?)";
	private static final String UPDATE_SQL = "UPDATE " + TID_MGMT + " SET STATUS = ? WHERE TID = ? AND PROG_ID = ?";
	private static final String UPDATE_ROLLBACK_SQL = "UPDATE " + TID_MGMT
			+ " SET STATUS = ?, PREVIOUS_STATUS = ?  WHERE TID = ? AND PROG_ID = ?";
	private static final String EXISTS_SQL = "SELECT TID FROM " + TID_MGMT + " WHERE TID = '" + EXISTS
			+ "' AND PROG_ID = '" + EXISTS + "'";
	private static final String CREATE_PAYLOAD_SQL = "CREATE TABLE " + IDOC_PAYLOAD
			+ " ( TID varchar(255), DOCNUM varchar(255), DOCTYP varchar(255), IDOCTYP varchar(255), CIMTYP varchar(255), IDOC text,TIMESTAMP timestamp)";
	private static final String INSERT_PAYLOAD_SQL = "INSERT INTO " + IDOC_PAYLOAD
			+ " (TID, DOCNUM, DOCTYP, IDOCTYP, CIMTYP, IDOC, TIMESTAMP) VALUES (?,?,?,?,?,?,?)";
	private static final String EXISTS_PAYLOAD_SQL = "SELECT TID FROM " + IDOC_PAYLOAD + " WHERE TID = '" + EXISTS
			+ "' AND DOCNUM = '" + EXISTS + "'";

	private static final int MAX_RETRY_COUNT = 3;

	private String programId;
	private BasicDataSource datasource;
	private SAPJcoConnection conProps;

	private enum TIDTableStatus {
		DOESNT_EXIST, MISSING_EXISTS_ROW, READY;
	}

	/**
	 * This method will perform initialization activities to setup TID Management.
	 * 
	 * @param connection
	 * @param programId
	 */
	public void initialize(SAPJcoConnection connection, String programId) {
		try {
			this.conProps = connection;
			if (!isDBNotRequired()) {
				createTable(connection);

				this.datasource = createDataSource(connection);
			}
			this.programId = programId;
		} catch (SQLException e) {
			throw new ConnectorException("Unable to setup TID management", e);
		}
	}

	@Override
	public boolean checkTID(JCoServerContext serverCtx, String tid) {
		if (isDBNotRequired()) {
			return true;
		} else {
			int retryCount = 0;
			while (true) {
				try (Connection conn = datasource.getConnection();
						PreparedStatement insertPS = conn.prepareStatement(INSERT_SQL);) {
					// attempt to insert tid record
					insertPS.setString(1, tid);
					insertPS.setString(2, programId);
					insertPS.setString(3, CHECKED);
					insertPS.executeUpdate();
					return true;
				} catch (SQLIntegrityConstraintViolationException e) {
					// Duplicate primary keys in the database. Return false to indicate that we
					// already received this TID.
					LogUtil.infoDebug(logger, "Not handling TID, TID= " + tid + " (" + e.getMessage() + ")", e);
					return false;
				} catch (SQLException e) {
					if (retryCount == MAX_RETRY_COUNT) {
						logger.log(Level.SEVERE, "Unable to connect to TID Database : "+e.getMessage());
						return false;
					}
					retryCount++;
					logger.log(Level.WARNING, "checkTID Retrying Attempt : " + retryCount);
				} catch (Exception e) {
					throw new ConnectorException(e);
				}
			}

		}

	}

	@Override
	public void commit(JCoServerContext serverCtx, String tid) {
		if (!isDBNotRequired()) {
			if (StringUtil.isBlank(tid)) {
				throw new ConnectorException("Unable to find IDocs associated with TID=" + tid);
			}
			updateStatus(tid, COMMITED);
		}
	}

	@Override
	public void confirmTID(JCoServerContext serverCtx, String tid) {
		if (!isDBNotRequired()) {
			updateStatus(tid, CONFIRMED);
		}
	}

	@Override
	public void rollback(JCoServerContext serverCtx, String tid) {
		if (!isDBNotRequired()) {
			int retryCount = 0;
			while(true) {
				try (Connection conn = datasource.getConnection();
						PreparedStatement rollbackUpdatePS = conn.prepareStatement(UPDATE_ROLLBACK_SQL);) {
					rollbackUpdatePS.setString(1, ROLLBACK);
					rollbackUpdatePS.setString(2, ROLLBACK);
					rollbackUpdatePS.setString(3, tid);
					rollbackUpdatePS.setString(4, programId);
					rollbackUpdatePS.executeUpdate();
					return;
				} catch (SQLException e) {
					if (retryCount == MAX_RETRY_COUNT) {
						logger.log(Level.SEVERE, "Unable to delete entry for TID: {0}", tid);
						throw new ConnectorException(e);
					}
					retryCount++;
					logger.log(Level.WARNING, "rollback Retrying Attempt : " + retryCount);
				}
			}
			
		}
	}

	/**
	 * This method will create and return basicDataSource from connection
	 * properties.
	 * 
	 * @param con
	 * @return basicDataSource
	 */
	public static BasicDataSource createDataSource(SAPJcoConnection con) {
		BasicDataSource bds = new BasicDataSource();
		bds.setDriverClassName(con.getDbDriverClassName());
		bds.setUsername(con.getDatabaseUserName());
		bds.setPassword(con.getDatabasePassword());
		bds.setUrl(createConnectionURL(con));
		bds.setTestOnBorrow(true);
		bds.setValidationQuery(EXISTS_SQL);
		if (con.getMaximumConnections() != null) {
			// BasicDataSource defaults to 8 if not specified.
			bds.setMaxIdle(con.getMaximumConnections());
		}
		if (con.getMinimumConnections() != null) {
			// BasicDataSource defaults to 0 if not specified.
			bds.setMinIdle(con.getMinimumConnections());
		}

		return bds;
	}

	/**
	 * This method will form the connectionURL based on the database selected.
	 * 
	 * @param con
	 * @return connectionURL
	 */
	private static String createConnectionURL(SAPJcoConnection con) {
		String urlFormat = con.getDbUrl();
		urlFormat = MessageFormat.format(urlFormat, con.getDatabaseHost(), String.valueOf(con.getDatabasePort()),
				con.getDatabaseName());
		if (StringUtil.isNotBlank(con.getAdditionalOptions())) {
			urlFormat = urlFormat + con.getAdditionalOptions();
		}
		return urlFormat;
	}

	/**
	 * This method will create the TID_MGMT table if not exists.
	 * 
	 * @param con
	 * @throws SQLException
	 */
	private static void createTIDTable(Connection con) throws SQLException {
		TIDTableStatus tableStatus = checkTableStatus(con, EXISTS_SQL);
		// first see if we need to create the table
		if (TIDTableStatus.DOESNT_EXIST == tableStatus) {
			Statement stmt = null;
			try {
				stmt = con.createStatement();
				stmt.executeUpdate(CREATE_SQL);
			} finally {
				DBUtil.closeQuietly(stmt);
			}
			logger.log(Level.INFO, "created table " + TID_MGMT);
		}

		// now check if we need to add the 'exists' row
		if (TIDTableStatus.READY != tableStatus) {
			// add the 'exists' row
			PreparedStatement insertPS = null;
			try {
				insertPS = con.prepareStatement(INSERT_SQL);
				insertPS.setString(1, EXISTS);
				insertPS.setString(2, EXISTS);
				insertPS.setString(3, DONT_DELETE);
				insertPS.executeUpdate();
			} finally {
				DBUtil.closeQuietly(insertPS);
			}
		}
	}

	/**
	 * This method will create the IDOC_PAYLOAD table if not exists.
	 * 
	 * @param con
	 * @throws SQLException
	 */
	private static void createPayloadTable(Connection con) throws SQLException {
		TIDTableStatus tableStatus = checkTableStatus(con, EXISTS_PAYLOAD_SQL);
		// first see if we need to create the table
		if (TIDTableStatus.DOESNT_EXIST == tableStatus) {
			Statement stmt = null;
			try {
				stmt = con.createStatement();
				stmt.executeUpdate(CREATE_PAYLOAD_SQL);
			} finally {
				DBUtil.closeQuietly(stmt);
			}
			logger.log(Level.INFO, "created table " + IDOC_PAYLOAD);
		}

		// now check if we need to add the 'exists' row
		if (TIDTableStatus.READY != tableStatus) {
			// add the 'exists' row
			PreparedStatement insertPS = null;
			try {
				insertPS = con.prepareStatement(INSERT_PAYLOAD_SQL);
				insertPS.setString(1, EXISTS);
				insertPS.setString(2, EXISTS);
				insertPS.setString(3, DONT_DELETE);
				insertPS.setString(4, DONT_DELETE);
				insertPS.setString(5, DONT_DELETE);
				insertPS.setString(6, DONT_DELETE);
				insertPS.setTimestamp(7, new Timestamp(new Date().getTime()));
				insertPS.executeUpdate();
			} finally {
				DBUtil.closeQuietly(insertPS);
			}
		}
	}

	/**
	 * This method will create the TID_MGMT table if it is not exists and insert the
	 * initial data.
	 * 
	 * @param con
	 * @throws SQLException
	 */
	private static void createTable(SAPJcoConnection con) throws SQLException {
		Connection conn = null;
		try {
			// using a non-pooled connection since the conn validation is based
			// on the tid table and it might not exist yet
			conn = getNonPooledConnection(con);
			createTIDTable(conn);
			if (con.getTidManagementOptions().equals(SAPJcoConstants.FULL)) {
				createPayloadTable(conn);
			}

		} finally {
			DBUtil.closeQuietly(conn);
		}
	}

	/**
	 * This method will insert the idoc payload details into database.
	 * 
	 * @param payloadDetails
	 */
	public void insertPayloadDetails(PayloadDetails payloadDetails) {
		int retryCount = 0;
		while (true) {
			try (Connection conn = datasource.getConnection();
					PreparedStatement insertPS = conn.prepareStatement(INSERT_PAYLOAD_SQL);) {
				insertPS.setString(1, payloadDetails.getTid());
				insertPS.setString(2, payloadDetails.getDocNumber());
				insertPS.setString(3, payloadDetails.getDocType());
				insertPS.setString(4, payloadDetails.getIdocType());
				insertPS.setString(5, payloadDetails.getExtension());
				insertPS.setString(6, payloadDetails.getIdoc());
				insertPS.setTimestamp(7, payloadDetails.getTimestamp());
				insertPS.executeUpdate();
				return;
			} catch (SQLException e) {
				if (retryCount == MAX_RETRY_COUNT) {
					throw new ConnectorException("Failed to store payload into database : " + e.getMessage());
				}
				retryCount++;
				logger.log(Level.WARNING, "insertPayloadDetails Retrying Attempt : " + retryCount);
			}
		}
	}

	/**
	 * This method will create and return the non pooled database connection.
	 * 
	 * @param con
	 * @return dbNonPooledConnection
	 * @throws SQLException
	 */
	private static Connection getNonPooledConnection(SAPJcoConnection con) throws SQLException {
		try {
			Class.forName(con.getDbDriverClassName());
			return DriverManager.getConnection(createConnectionURL(con), con.getDatabaseUserName(),
					con.getDatabasePassword());
		} catch (ClassNotFoundException e) {
			throw new SQLException(String.format(
					"Unable to instantiate driver class %s, please make sure the appropriate jar files are loaded.",
					con.getDbDriverClassName()), e);
		}
	}

	/**
	 * This method will check and return the TIDTableStatus.
	 * 
	 * @param conn
	 * @return TIDTableStatus
	 * @throws SQLException
	 */
	private static TIDTableStatus checkTableStatus(Connection conn, String existsSql) throws SQLException {
		Statement stmt = null;
		ResultSet rs = null;
		TIDTableStatus tidTableStatus = null;
		try {
			stmt = conn.createStatement();
			try {
				rs = stmt.executeQuery(existsSql);
			} catch (SQLException e) {
				// ignore, implies table doesn't exist
				tidTableStatus = TIDTableStatus.DOESNT_EXIST;
			}
			if (rs != null) {
				tidTableStatus = (rs.next()) ? TIDTableStatus.READY : TIDTableStatus.MISSING_EXISTS_ROW;
			}
		} finally {
			DBUtil.closeQuietly(rs, stmt);
		}
		return tidTableStatus;
	}

	/**
	 * This method will perform programId updation on TID_MGMT table.
	 * 
	 * @param tid
	 * @param status
	 */
	private void updateStatus(String tid, String status) {
		int retryCount = 0;
		while (true) {
			try (Connection conn = datasource.getConnection();
					PreparedStatement updatePS = conn.prepareStatement(UPDATE_SQL);) {
				updatePS.setString(1, status);
				updatePS.setString(2, tid);
				updatePS.setString(3, programId);
				updatePS.executeUpdate();
				return;
			} catch (SQLException e) {

				if (retryCount == MAX_RETRY_COUNT) {
					logger.log(Level.SEVERE, "Unable to update status of {0} to {1} ", new String[] { tid, status });
					throw new ConnectorException(e);
				}
				retryCount++;
				logger.log(Level.WARNING, "updateStatus Retrying Attempt : " + retryCount);
			}
		}

	}

	/**
	 * This method is to close the datasource.
	 */
	public void close() {
		if (this.datasource != null) {
			try {
				this.datasource.close();
			} catch (SQLException sQLException) {
				// empty catch block
			}
			this.datasource = null;
		}
	}

	/**
	 * This method will return true if database is not required for transaction
	 * management.
	 * 
	 * @return boolean
	 */
	private boolean isDBNotRequired() {
		return conProps.getTidManagementOptions().equals(SAPJcoConstants.NONE);
	}

}
