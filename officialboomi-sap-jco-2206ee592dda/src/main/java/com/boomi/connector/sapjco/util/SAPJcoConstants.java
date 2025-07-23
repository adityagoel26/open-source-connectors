// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sapjco.util;

/**
 * @author kishore.pulluru
 *
 */
public class SAPJcoConstants {
	private SAPJcoConstants() {
	}

	public static final String AHOST = "AHOST";
	public static final String MHOST = "MHOST";
	public static final String EXECUTE = "EXECUTE";
	public static final String BUSINESS_OBJECT = "BUSINESS_OBJECT";
	public static final String BAPI = "BAPI";
	public static final String FUNCTION = "FUNCTION";
	public static final String SHORTTEXT = "SHORTTEXT";
	public static final String DELIMITER = "~";
	public static final String SEND = "SEND";
	public static final String IDOC = "IDoc";
	public static final String COMMIT_TXN = "commitTransaction";
	public static final String SUCCESS_RES_CODE = "200";
	public static final String SUCCESS_RES_MSG ="OK";
	public static final String ITEM = "item";
	public static final String LISTEN = "LISTEN";
	public static final String OBJTYPE = "OBJTYPE";
	public static final String WITH_OBJECT_NAMES = "WITH_OBJECT_NAMES";
	
	public static final String NONE = "None";
	public static final String MINIMUM = "Minimum";
	public static final String FULL = "Full";


	public static final String FUNCTION_NAME = "functionName";
	public static final String FUNCTION_TYPE = "functionType";
	public static final String JCO_TRACE_PATH = "jco.trace_path";
	public static final String JCO_TRACE_LEVEL = "jco.trace_level";

	public static final String CUSTOM_DB = "CUSTOM";
	public static final String SQL_SERVER_JTDS = "SQL_SERVER_JTDS";
	public static final String ORACLE = "ORACLE";
	public static final String MYSQL = "MYSQL";
	public static final String SQL_SERVER_MICROSOFT = "SQL_SERVER_MICROSOFT";
	public static final String SAP_HANA = "SAP_HANA";

	public static final String SQL_SERVER_JTDS_DRIVER_CLASS = "net.sourceforge.jtds.jdbc.Driver";
	public static final String ORACLE_DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";
	public static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
	public static final String SQL_SERVER_MICROSOFT_DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	public static final String SAP_HANA_DRIVER_CLASS = "com.sap.db.jdbc.Driver";

	public static final String SQL_SERVER_JTDS_CONNECTION_URL = "jdbc:jtds:sqlserver://{0}:{1}/{2}";// jdbc:jtds:sqlserver://host:123/dbname
	public static final String ORACLE_CONNECTION_URL = "jdbc:oracle:thin:@{0}:{1}:{2}";// jdbc:oracle:thin:@host:123:dbname
	public static final String MYSQL_CONNECTION_URL = "jdbc:mysql://{0}:{1}/{2}"; // jdbc:mysql://host:123/dbname
	public static final String SQL_SERVER_MICROSOFT_CONNECTION_URL = "jdbc:sqlserver://{0}:{1};database= {2}"; // jdbc:sqlserver://host:123;database=dbname
	public static final String SAP_HANA_CONNECTION_URL = "jdbc:sap://{0}:{1}/?databaseName= {2}";// jdbc:sap://host:123/?databaseName=dbname

	//Listener
	public static final String TID = "tid";
	public static final String STATUS = "status";
	public static final String IDOCTYPE = "idocType";
	public static final String RECEIVERPORT = "receiverPort";
	public static final String PARTNERRECEIVER = "partnerReceiver";
	public static final String TESTFLAG = "testFlag";
	public static final String SENDERPORT = "senderPort";
	public static final String PARTNERTYPE = "partnerType";
	public static final String PARTNERNUMBER = "partnerNumber";
	public static final String CREATEDDATE = "createdDate";
	public static final String CREATEDTIME = "createdTime";
	public static final String DATEPATTERN = "yyyyMMdd";
	public static final String TIMEPATTERN = "HHmmss.SSS";
	public static final String IDOCNUMBER = "idocNumber";
	public static final String LOGICALMESSAGEVARIENT = "logicalMessageVariant";
	public static final String MESSAGETYPE = "messageType";
	public static final String BASICTYPE = "basicType";
	public static final String EXTENSION = "extension";
	
	
}
