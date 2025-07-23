// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.databaseconnector.operations.get;

import oracle.jdbc.OracleType;
import oracle.sql.json.OracleJsonObject;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.util.DBv2JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.commons.text.StringEscapeUtils;
import org.postgresql.util.PGobject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.DOUBLE_QUOTE;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.OPEN_IN;

/**
 * The Class NamedParameterStatement.
 * 
 * @author swastik.vn
 */
public class NamedParameterStatement implements AutoCloseable {

	/** The statement this object is wrapping. */
	private final PreparedStatement statement;

	/**
	 * Maps parameter names to arrays of ints which are the parameter indices.
	 */
	private final Map<String, Object> indexMap;

	/** The parsed query. */
	private String parsedQuery;

	/**
	 * Gets the parsed query.
	 *
	 * @return the parsed query
	 */
	public String getParsedQuery() {
		return parsedQuery;
	}

	/**
	 * Creates a NamedParameterStatement. Wraps a call to
	 * c.{@link Connection#prepareStatement(java.lang.String) prepareStatement}.
	 *
	 * @param connection the database connection
	 * @param query      the parameterized query
	 * @param data       the data
	 * @throws SQLException if the statement could not be created
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	public NamedParameterStatement(Connection connection, String query, ObjectData data)
			throws SQLException, IOException {
		this.indexMap = new HashMap<>();
		this.parsedQuery = inclauseParse(buildQueryForNClause(query, data), indexMap);
		this.statement = connection.prepareStatement(parsedQuery);
	}

	/**
	 * Instantiates a new named parameter statement.
	 *
	 * @param connection the connection
	 * @param query      the query
	 * @throws SQLException the SQL exception
	 */
	public NamedParameterStatement(Connection connection, String query) throws SQLException {
		this.indexMap = new HashMap<>();
		this.parsedQuery = parse(query, indexMap);
		this.statement = connection.prepareStatement(parsedQuery);
	}

	/**
	 * Parses a query with named parameters. The parameter-index mappings are put
	 * into the map, and the parsed query is returned.
	 *
	 * @param query    query to parse
	 * @param paramMap map to hold parameter-index mappings
	 * @return the parsed query
	 */
	public static final String parse(String query, Map<String, Object> paramMap) {
		query = query.trim();
		paramMap.clear();
		int index = 1;
		String[] spcChar = query.split("\\s");
		for (int i = 0; i <= spcChar.length - 1; i++) {
			String indxName = "";
			if (spcChar[i].contains("$")){
			if (spcChar[i].startsWith("($") || spcChar[i].startsWith("$")) {
				 indxName = spcChar[i];
				if (indxName.endsWith(")") || indxName.endsWith(";")) {
					indxName = indxName.substring(1, indxName.length() - 1);
				} else
					indxName = indxName.substring(1, indxName.length());

				spcChar[i] = "?";
				List<Integer> indexList = (List<Integer>) paramMap.get(indxName.toUpperCase());
				if (indexList == null) {
					indexList = new LinkedList<>();
					paramMap.put(indxName.toUpperCase(), indexList);
				}
				indexList.add(index);

				index++;
			}
			else if(spcChar[i].contains("=$") || spcChar[i].contains(">$") || spcChar[i].contains(">=$")|| spcChar[i].contains("<$")
					||spcChar[i].contains("<=$") || spcChar[i].contains("!=$")|| spcChar[i].contains("%$")|| spcChar[i].contains("<>$")){
				String[] namedParam = spcChar[i].split("\\$");
				spcChar[i] = namedParam[0] + "?";
				indxName = "$" + namedParam[1];

				/* First if removes the trailing semicolon and next if condition removes the parentheses to get the
				index name
				and appends the right parenthesis to the parsed query string */
				int indxLength = indxName.length();
				if (indxName.endsWith(");")) {
					indxName = indxName.substring(1, indxLength - 2);
					spcChar[i] = spcChar[i].concat(")");
				} else if (indxName.endsWith(";")) {
					indxName = indxName.substring(1, indxLength - 1);
				} else if (indxName.endsWith(")")) {
					indxName = indxName.substring(1, indxLength - 1);
					spcChar[i] = spcChar[i].concat(")");
				} else {
					indxName = indxName.substring(1);
				}

				List<Integer> indexList = (List<Integer>) paramMap.computeIfAbsent(indxName.toUpperCase(),
						k -> new LinkedList<Integer>());
				indexList.add(index);

				index++;
			}
			}
		}
		StringBuilder parsedQuery = new StringBuilder("");

		for (String str : spcChar) {
			parsedQuery.append(str).append(" ");
		}
		for (Map.Entry<String, Object> entry : paramMap.entrySet()) {
			List list = (List) entry.getValue();
			int[] indexes = new int[list.size()];
			int i = 0;
			for (Iterator iterator = list.iterator(); iterator.hasNext();) {
				Integer x = (Integer) iterator.next();
				indexes[i++] = x.intValue();
			}
			entry.setValue(indexes);
		}

		return parsedQuery.toString();
	}

	/**
	 * Parses a query with named parameters. The parameter-index mappings are put
	 * into the map, and the parsed query is returned.
	 *
	 * @param query    query to parse
	 * @param paramMap map to hold parameter-index mappings
	 * @return the parsed query
	 */
	public static final String inclauseParse(String query, Map<String, Object> paramMap) {
		StringBuilder parsedQuery = new StringBuilder("");
		if(query != null) {
			query = query.trim();
			paramMap.clear();
			int index = 1;
			String[] splittedQuery = query.split("\\s+");
			for (int i = 0; i <= splittedQuery.length - 1; i++) {
				index = fillParams(i, index, splittedQuery, paramMap);
			}
			

			for (String str : splittedQuery) {
				parsedQuery.append(str).append(" ");
			}

			// replace the lists of Integer objects with arrays of ints
			for (Map.Entry<String, Object> entry : paramMap.entrySet()) {
				List<Integer> list = (List) entry.getValue();
				int[] indexes = new int[list.size()];
				int i = 0;
				for(Integer x :list) {
					indexes[i++] = x;
				}
				entry.setValue(indexes);
			}
		}
		return parsedQuery.toString();

	}

	/**
	 * Gets the keys list.
	 *
	 * @param query the query
	 * @return the keys list
	 */
	private List<String> getKeysList(String query) {
		query = query.trim();
		String[] spcST = query.split("\\s+");
		List<String> paramList = new ArrayList<>();
		for (int j = 0; j <= spcST.length - 1; j++) {
			String[] spcChar = spcST[j].split(",");
			for (int i = 0; i <= spcChar.length - 1; i++) {
				if (spcChar[i].startsWith("($") ||spcChar[i].startsWith("$") || 
						spcChar[i].startsWith("IN($") || spcChar[i].startsWith("in($")) {
					paramList.add(this.formatString(spcChar[i]));
				}
			}

		}
		return paramList;
	}

	/**
	 * Builds the query for N clause.
	 *
	 * @param query   the query
	 * @param objdata the objdata
	 * @return the string
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private String buildQueryForNClause(String query, ObjectData objdata) throws IOException {
		ObjectReader  reader    = DBv2JsonUtil.getObjectReader();
		StringBuilder replacing = new StringBuilder();
		try (InputStream is = objdata.getData();) {
			JsonNode json = null;
			if (is.available() == 0 || null == (json = reader.readTree(is))) {
				throw new ConnectorException("Please check the Input Request!!!");
			}
			String stringUppercase = null;
			if (Pattern.compile(DatabaseConnectorConstants.CAPS_IN).matcher(query).find()) {
				stringUppercase = query.replaceAll("IN\\s+\\(", OPEN_IN);
			} else if (Pattern.compile(DatabaseConnectorConstants.SMALL_IN).matcher(query).find()) {
				stringUppercase = query.replaceAll("in\\(", "IN\\(");
				stringUppercase = stringUppercase.replaceAll("in\\s+\\(", OPEN_IN);
			}
			
			Iterator<String> iter = getKeysList(query).iterator();
			while (iter.hasNext()) {
				String key = iter.next();
				if (json.get(key) instanceof ArrayNode) {
					int arraySize = json.get(key).size();
					for (int j = 0; j < arraySize; j++) {
						replacing.append("$").append(key);
						if (j < arraySize - 1) {
							replacing.append(",");
						}
					}
					if(stringUppercase != null)
						stringUppercase = stringUppercase.replace("$" + key, replacing);
					replacing.setLength(0);
				}

			}
			return stringUppercase;

		}
	}

	/**
	 * Returns the indexes for a parameter.
	 * 
	 * @param name parameter name
	 * @return parameter indexes
	 * @throws IllegalArgumentException if the parameter does not exist
	 */
	private int[] getIndexes(String name) {
		int[] indexes = (int[]) indexMap.get(name);
		if (indexes == null) {
			throw new IllegalArgumentException("Parameter not found: " + name);
		}
		return indexes;
	}

	/**
	 * Sets a parameter.
	 * 
	 * @param name  parameter name
	 * @param value parameter value
	 * @throws SQLException             if an error occurred
	 * @throws IllegalArgumentException if the parameter does not exist
	 * @see PreparedStatement#setObject(int, java.lang.Object)
	 */
	public void setObject(String name, Object value) throws SQLException {
		int[] indexes = getIndexes(name);
		for (int i = 0; i < indexes.length; i++) {
			this.statement.setObject(indexes[i], value);
		}
	}

	/**
	 * Sets a parameter.
	 *
	 * @param name  parameter name
	 * @param value parameter value
	 * @throws SQLException             if an error occurred
	 * @see PreparedStatement#setString(int, java.lang.String)
	 */
	public void setString(String name, String value) throws SQLException {
		int[] indexes = getIndexes(name.toUpperCase());
		for (int i = 0; i < indexes.length; i++) {
			this.statement.setString(indexes[i], value);
		}
	}

	/**
	 * Sets the given string value for all occurrences of the specified parameter name in the prepared statement.
	 *
	 * @param parameterName The name of the parameter.
	 * @param value         The value to set for the parameter.
	 * @throws IllegalArgumentException If parameterName is null or empty.
	 * @throws SQLException             If a database access error occurs.
	 */
	public void setNString(String parameterName, String value) throws SQLException {
		int[] indexes = getIndexes(parameterName.toUpperCase());
		for (int index : indexes) {
			this.statement.setNString(index, value);
		}
	}

	/**
	 * Sets the string update.
	 *
	 * @param key   the key
	 * @param array the array
	 * @throws SQLException the SQL exception
	 */
	public void setStringArray(String key, ArrayNode array) throws SQLException {
		int[] indexes = getIndexes(key.toUpperCase());
		Iterator<JsonNode> slaidsIterator = array.elements();
		for (int i = 0; i < indexes.length; i++) {
			JsonNode node = null;
			if (slaidsIterator.hasNext()) {
				node = slaidsIterator.next();
			} else {
				slaidsIterator = array.elements();
				node = slaidsIterator.next();
			}
			this.statement.setString(indexes[i], node.toString().replace(DOUBLE_QUOTE, ""));
		}
	}

	/**
	 * Sets the nvarchar update.
	 *
	 * @param key   the key
	 * @param array the array
	 * @throws SQLException the SQL exception
	 */
	public void setNvarcharArray(String key, ArrayNode array) throws SQLException {
		int[] indexes = getIndexes(key.toUpperCase());
		Iterator<JsonNode> slaidsIterator = array.elements();
		for (int i = 0; i < indexes.length; i++) {
			JsonNode node = null;
			if (slaidsIterator.hasNext()) {
				node = slaidsIterator.next();
			} else {
				slaidsIterator = array.elements();
				node = slaidsIterator.next();
			}
			this.statement.setNString(indexes[i],
					StringEscapeUtils.unescapeJava(node.toString().replace(DOUBLE_QUOTE, "")));
		}
	}

	/**
	 * Sets a parameter.
	 *
	 * @param name  parameter name
	 * @param value parameter value
	 * @throws SQLException             if an error occurred
	 * @throws IllegalArgumentException if the parameter does not exist
	 * @see PreparedStatement#setInt(int, int)
	 */
	public void setInt(String name, int value) throws SQLException {
		int[] indexes = getIndexes(name.toUpperCase());
		for (int i = 0; i < indexes.length; i++) {
			this.statement.setInt(indexes[i], value);
		}
	}

	/**
	 * This method will update the indexes of the placeholder present in the query
	 * if the Query is changed to take multiple parameters based on the input.
	 *
	 * @param key   the key
	 * @param array the array
	 * @throws SQLException the SQL exception
	 */
	public void setIntArray(String key, ArrayNode array) throws SQLException {

		int[] indexes = getIndexes(key.toUpperCase());
		Iterator<JsonNode> slaidsIterator = array.elements();
		for (int i = 0; i < indexes.length; i++) {
			JsonNode node = null;
			if (!slaidsIterator.hasNext()) {
				slaidsIterator = array.elements();
			}
			node = slaidsIterator.next();
			this.statement.setInt(indexes[i], Integer.parseInt(node.toString().replace(DOUBLE_QUOTE, "")));
		}
	}

	/**
	 * Sets a parameter.
	 * 
	 * @param name  parameter name
	 * @param value parameter value
	 * @throws SQLException             if an error occurred
	 * @throws IllegalArgumentException if the parameter does not exist
	 * @see PreparedStatement#setInt(int, int)
	 */
	public void setLong(String name, long value) throws SQLException {
		int[] indexes = getIndexes(name.toUpperCase());
		for (int i = 0; i < indexes.length; i++) {
			this.statement.setLong(indexes[i], value);
		}
	}

	/**
	 * Sets a parameter.
	 * 
	 * @param name  parameter name
	 * @param value parameter value
	 * @throws SQLException             if an error occurred
	 * @throws IllegalArgumentException if the parameter does not exist
	 * @see PreparedStatement#setTimestamp(int, java.sql.Timestamp)
	 */
	public void setTimestamp(String name, Timestamp value) throws SQLException {
		int[] indexes = getIndexes(name.toUpperCase());
		for (int i = 0; i < indexes.length; i++) {
			this.statement.setTimestamp(indexes[i], value);
		}
	}

	/**
	 * Sets the date.
	 *
	 * @param name  the name
	 * @param value the value
	 * @throws SQLException the SQL exception
	 */
	public void setDate(String name, Date value) throws SQLException {
		int[] indexes = getIndexes(name.toUpperCase());
		for (int i = 0; i < indexes.length; i++) {
			this.statement.setDate(indexes[i], value);
		}
	}

	/**
	 * Sets the date update.
	 *
	 * @param key   the key
	 * @param array the array
	 * @throws SQLException the SQL exception
	 */
	public void setDateArray(String key, ArrayNode array) throws SQLException {
		int[] indexes = getIndexes(key.toUpperCase());
		Iterator<JsonNode> slaidsIterator = array.elements();
		for (int i = 0; i < indexes.length; i++) {
			JsonNode node = null;
			if (slaidsIterator.hasNext()) {
				node = slaidsIterator.next();
			} else {
				slaidsIterator = array.elements();
				node = slaidsIterator.next();
			}
			try {
				this.statement.setDate(indexes[i], Date.valueOf(node.toString().replace(DOUBLE_QUOTE, "")));
			}catch(IllegalArgumentException e) {
				throw new IllegalArgumentException(DatabaseConnectorConstants.INVALID_ERROR +e);
			}
		}
	}

	/**
	 * Sets the time.
	 *
	 * @param name  the name
	 * @param value the value
	 * @throws SQLException the SQL exception
	 */
	public void setTime(String name, Time value) throws SQLException {
		int[] indexes = getIndexes(name.toUpperCase());
		for (int i = 0; i < indexes.length; i++) {
			this.statement.setTime(indexes[i], value);
		}
	}

	/**
	 * Sets the time update.
	 *
	 * @param key   the key
	 * @param array the array
	 * @throws SQLException the SQL exception
	 */
	public void setTimeArray(String key, ArrayNode array) throws SQLException {
		int[] indexes = getIndexes(key.toUpperCase());
		Iterator<JsonNode> slaidsIterator = array.elements();
		for (int i = 0; i < indexes.length; i++) {
			JsonNode node = null;
			if (slaidsIterator.hasNext()) {
				node = slaidsIterator.next();
			} else {
				slaidsIterator = array.elements();
				node = slaidsIterator.next();
			}
			this.statement.setTime(indexes[i], Time.valueOf(node.toString().replace(DOUBLE_QUOTE, "")));
		}
	}

	/**
	 * Sets the object.
	 *
	 * @param i          the i
	 * @param jsonObject the json object
	 * @throws SQLException the SQL exception
	 */
	public void setObject(int i, PGobject jsonObject) throws SQLException {
		this.statement.setObject(i, jsonObject);
	}

	/**
	 * Sets the object.
	 *
	 * @param i      the i
	 * @param object the object
	 * @throws SQLException the SQL exception
	 */
	public void setObject(int i, OracleJsonObject object) throws SQLException {
		this.statement.setObject(i, object, OracleType.JSON);
	}

	/**
	 * Sets the boolean.
	 *
	 * @param name  the name
	 * @param value the value
	 * @throws SQLException the SQL exception
	 */
	public void setBoolean(String name, Boolean value) throws SQLException {
		int[] indexes = getIndexes(name.toUpperCase());
		for (int i = 0; i < indexes.length; i++) {
			this.statement.setBoolean(indexes[i], value);
		}

	}

	/**
	 * Sets the boolean update.
	 *
	 * @param key   the key
	 * @param array the array
	 * @throws SQLException the SQL exception
	 */
	public void setBooleanArray(String key, ArrayNode array) throws SQLException {
		int[] indexes = getIndexes(key.toUpperCase());
		Iterator<JsonNode> slaidsIterator = array.elements();
		for (int i = 0; i < indexes.length; i++) {
			JsonNode node = null;
			if (slaidsIterator.hasNext()) {
				node = slaidsIterator.next();
			} else {
				slaidsIterator = array.elements();
				node = slaidsIterator.next();
			}
			this.statement.setBoolean(indexes[i], Boolean.valueOf(node.toString().replace(DOUBLE_QUOTE, "")));
		}
	}

	/**
	 * Sets the query for Exec(?).
	 *
	 * @param i     the i
	 * @param query the query
	 * @throws SQLException the SQL exception
	 */
	public void setExec(int i, String query) throws SQLException {
		this.statement.setString(1, query);
	}

	/**
	 * Execute.
	 *
	 * @throws SQLException the SQL exception
	 */
	public void execute() throws SQLException {
		this.statement.execute();
	}

	/**
	 * Execute query.
	 *
	 * @return the result set
	 * @throws SQLException the SQL exception
	 */
	public ResultSet executeQuery() throws SQLException {
		return this.statement.executeQuery();
	}

	/**
	 * Sets the max rows.
	 *
	 * @param intValue the new max rows
	 * @throws SQLException the SQL exception
	 */
	public void setMaxRows(int intValue) throws SQLException {
		this.statement.setMaxRows(intValue);

	}

	/**
	 * Sets the max field size.
	 *
	 * @param intValue the new max field size
	 * @throws SQLException the SQL exception
	 */
	public void setMaxFieldSize(int intValue) throws SQLException {
		this.statement.setMaxFieldSize(intValue);

	}

	/**
	 * Sets the null.
	 *
	 * @param i       the i
	 * @param sqlType the sql type
	 * @throws SQLException the SQL exception
	 */
	public void setNull(int i, int sqlType) throws SQLException {
		this.statement.setNull(i, sqlType);

	}

	/**
	 * Sets the clob.
	 *
	 * @param key    the key
	 * @param string the string
	 * @throws SQLException the SQL exception
	 */
	public void setClob(String key, Reader string) throws SQLException {
		int[] indexes = getIndexes(key.toUpperCase());
		for (int i = 0; i < indexes.length; i++) {
			this.statement.setClob(indexes[i], string);
		}
	}

	@Override
	public void close() throws SQLException {
		this.statement.close();
	}

	/**
	 * Sets the long update.
	 *
	 * @param key   the key
	 * @param array the array
	 * @throws SQLException the SQL exception
	 */
	public void setLongArray(String key, ArrayNode array) throws SQLException {
		int[] indexes = getIndexes(key.toUpperCase());
		Iterator<JsonNode> slaidsIterator = array.elements();
		for (int i = 0; i < indexes.length; i++) {
			JsonNode node = null;
			if (slaidsIterator.hasNext()) {
				node = slaidsIterator.next();
			} else {
				slaidsIterator = array.elements();
				node = slaidsIterator.next();
			}
			this.statement.setLong(indexes[i], Long.parseLong(node.toString().replace(DOUBLE_QUOTE, "")));
		}
	}

	/**
	 * Sets the float update.
	 *
	 * @param key   the key
	 * @param array the array
	 * @throws SQLException the SQL exception
	 */
	public void setFloatArray(String key, ArrayNode array) throws SQLException {
		int[] indexes = getIndexes(key.toUpperCase());
		Iterator<JsonNode> slaidsIterator = array.elements();
		for (int i = 0; i < indexes.length; i++) {
			JsonNode node = null;
			if (slaidsIterator.hasNext()) {
				node = slaidsIterator.next();
			} else {
				slaidsIterator = array.elements();
				node = slaidsIterator.next();
			}
			this.statement.setFloat(indexes[i], Float.parseFloat(node.toString().replace(DOUBLE_QUOTE, "")));
		}
	}

	/**
	 * Sets the double update.
	 *
	 * @param key   the key
	 * @param array the array
	 * @throws SQLException the SQL exception
	 */
	public void setDoubleArray(String key, ArrayNode array) throws SQLException {
		int[] indexes = getIndexes(key.toUpperCase());
		Iterator<JsonNode> slaidsIterator = array.elements();
		for (int i = 0; i < indexes.length; i++) {
			JsonNode node = null;
			if (slaidsIterator.hasNext()) {
				node = slaidsIterator.next();
			} else {
				slaidsIterator = array.elements();
				node = slaidsIterator.next();
			}
			this.statement.setDouble(indexes[i], Double.parseDouble(node.toString().replace(DOUBLE_QUOTE, "")));
		}
	}

	/**
	 * Sets the Blob update.
	 *
	 * @param key   the key
	 * @param array the array
	 * @throws SQLException the SQL exception
	 * @throws IOException 
	 */
	public void setBlobArray(String key, ArrayNode array) throws SQLException, IOException {
		int[] indexes = getIndexes(key.toUpperCase());
		Iterator<JsonNode> slaidsIterator = array.elements();
		for (int i = 0; i < indexes.length; i++) {
			JsonNode node = null;
			if (slaidsIterator.hasNext()) {
				node = slaidsIterator.next();
			} else {
				slaidsIterator = array.elements();
				node = slaidsIterator.next();
			}
			String value = node.toString().replace(DOUBLE_QUOTE, "");
			try(InputStream stream = new ByteArrayInputStream(value.getBytes());){
			this.statement.setBlob(indexes[i], stream);
			}
		}
	}

	/**
	 * Sets the Timestamp update.
	 *
	 * @param key   the key
	 * @param array the array
	 * @throws SQLException the SQL exception
	 */
	public void setTimeStampArray(String key, ArrayNode array) throws SQLException {
		int[] indexes = getIndexes(key.toUpperCase());
		Iterator<JsonNode> slaidsIterator = array.elements();
		for (int i = 0; i < indexes.length; i++) {
			JsonNode node = null;
			if (slaidsIterator.hasNext()) {
				node = slaidsIterator.next();
			} else {
				slaidsIterator = array.elements();
				node = slaidsIterator.next();
			}
			this.statement.setTimestamp(indexes[i], Timestamp.valueOf(node.toString().replace(DOUBLE_QUOTE, "")));
		}
	}

	/**
	 * Sets a parameter.
	 * 
	 * @param name  parameter name
	 * @param value parameter value
	 * @throws SQLException             if an error occurred
	 * @throws IllegalArgumentException if the parameter does not exist
	 * @see PreparedStatement#setInt(int, int)
	 */
	public void setFloat(String name, float value) throws SQLException {
		int[] indexes = getIndexes(name.toUpperCase());
		for (int i = 0; i < indexes.length; i++) {
			this.statement.setFloat(indexes[i], value);
		}
	}

	/**
	 * Sets a parameter.
	 * 
	 * @param name  parameter name
	 * @param value parameter value
	 * @throws SQLException             if an error occurred
	 * @throws IllegalArgumentException if the parameter does not exist
	 * @see PreparedStatement#setInt(int, int)
	 */
	public void setDouble(String name, double value) throws SQLException {
		int[] indexes = getIndexes(name.toUpperCase());
		for (int i = 0; i < indexes.length; i++) {
			this.statement.setDouble(indexes[i], value);
		}
	}

	/**
	 * Sets a parameter.
	 *
	 * @param name  parameter name
	 * @param value parameter value
	 * @throws SQLException             if an error occurred
	 * @throws IllegalArgumentException if the parameter does not exist
	 * @see PreparedStatement#setInt(int, int)
	 */
	public void setBigDecimal(String name, BigDecimal value) throws SQLException {
		int[] indexes = getIndexes(name.toUpperCase());
		for (int i = 0; i < indexes.length; i++) {
			this.statement.setBigDecimal(indexes[i], value);
		}
	}

	/**
	 * Sets a parameter.
	 * 
	 * @param name  parameter name
	 * @param value parameter value
	 * @throws SQLException             if an error occurred
	 * @throws IOException 
	 * @throws IllegalArgumentException if the parameter does not exist
	 * @see PreparedStatement#setInt(int, int)
	 */
	public void setBlob(String name, String value) throws SQLException, IOException {
		int[] indexes = getIndexes(name.toUpperCase());
		for (int i = 0; i < indexes.length; i++) {
			try(InputStream stream = new ByteArrayInputStream(value.getBytes());){
			this.statement.setBlob(indexes[i], stream);
			}
		}
	}

	private String formatString(String value) {
		if (value.endsWith(";")) {
			value = value.replace(";", "");
		}
		if (value.startsWith("($")) {
			value = value.replace("($", "");
		}
		if (value.startsWith("IN($")) {
			value = value.replace("IN($", "");
		}
		if (value.startsWith("in($")) {
			value = value.replace("in($", "");
		}
		if (value.startsWith("$")) {
			value = value.replace("$", "");
		}
		if (value.contains(")") || value.contains(",")) {
			int counter = 0;
			for (Character ch : value.toCharArray()) {
				if(ch==')')
				{
					break;
				}
				else
				{
					counter = counter+1;
				}
			}
			value= value.substring(0, counter);
			}
		return value;
	}
	
	private static int fillParams(int i, int index, String[] splittedQuery, Map<String, Object> paramMap) {
		
		if (splittedQuery[i].startsWith("IN($")|| splittedQuery[i].startsWith("IN ($")) {
			String[] inName = splittedQuery[i].split(",");
			String indxName = inName[0];
			if(indxName.endsWith(");")) {
				 indxName = indxName.substring(4, indxName.length() - 2);
			 }
			else if (indxName.endsWith(")") || indxName.endsWith(";")) {
				indxName = indxName.substring(4, indxName.length() - 1);
			} else
				indxName = indxName.substring(4);

			StringBuilder name = new StringBuilder();
			name.append("IN(");
			for (int k = 0; k < inName.length; k++) {
				if(k==inName.length-1) {
					String[] awd = splittedQuery[i].split("\\)");
					if(awd.length>1 || splittedQuery[i].contains("))")) {
						StringBuilder v = new StringBuilder(splittedQuery[i].split("\\)", 2)[1]);
						name.append("?)" + v) ;
					}
					else {
						name.append("?)");
					}
						
				}
				else {
					name.append("?,");						
				}
				List<Integer> indexList = (List<Integer>) paramMap.get(indxName.toUpperCase());
				if (indexList == null) {
					indexList = new LinkedList<>();
					paramMap.put(indxName.toUpperCase(), indexList);
				}
				indexList.add(index);

				index++;
			}

			splittedQuery[i] = name.toString();

		}
		index = buildParamNameforInClause(i, index, splittedQuery, paramMap);
		return index;
	
	}
	
	private static int buildParamNameforInClause(int i, int index, String[] splittedQuery, Map<String, Object> paramMap) {
		String indxName = "";
		if (splittedQuery[i].contains("$")){
		if(splittedQuery[i].startsWith("($") || splittedQuery[i].startsWith("$") ) {
			 indxName = splittedQuery[i];
			 if(indxName.endsWith(");")) {
				 indxName = indxName.substring(1, indxName.length() - 2);
			 }
			 else if (indxName.endsWith(")") || indxName.endsWith(";")) {
				indxName = indxName.substring(1, indxName.length() - 1);
			} else
				indxName = indxName.substring(1, indxName.length());

			splittedQuery[i] = "?";
			List<Integer> indexList = (List<Integer>) paramMap.get(indxName.toUpperCase());
			if (indexList == null) {
				indexList = new LinkedList<>();
				paramMap.put(indxName.toUpperCase(), indexList);
			}
			indexList.add(index);

			index++;
		}
		else if(splittedQuery[i].contains("=$") || splittedQuery[i].contains(">$") || splittedQuery[i].contains(">=$")|| splittedQuery[i].contains("<$")
				||splittedQuery[i].contains("<=$") || splittedQuery[i].contains("!=$")|| splittedQuery[i].contains("%$")|| splittedQuery[i].contains("<>$")){
			String[] a = splittedQuery[i].split("\\$");
			String b = a[0] + "?" ;
			splittedQuery[i] = b;
			 indxName = "$"+a[1];
				if (indxName.endsWith(")") || indxName.endsWith(";")) {
					indxName = indxName.substring(1, indxName.length() - 1);
				} else
					indxName = indxName.substring(1, indxName.length());
				List<Integer> indexList = (List<Integer>) paramMap.get(indxName.toUpperCase());
				if (indexList == null) {
					indexList = new LinkedList<>();
					paramMap.put(indxName.toUpperCase(), indexList);
				}
				indexList.add(index);

				index++;

		}
		}
		return index;
	}
	/**
	 * Sets the fetch size.
	 *
	 * @param intValue the new fetch size
	 * @throws SQLException the SQL exception
	 */
	public void setFetchSize(int intValue) throws SQLException {
		this.statement.setFetchSize(intValue);

	}
	
	/**
	 * Sets the Query Timeout.
	 *
	 * @param intValue the new Query Timeout
	 * @throws SQLException the SQL exception
	 */
	public void setReadTimeout(int readTimeout) throws SQLException {
		this.statement.setQueryTimeout(readTimeout);

	}
}
