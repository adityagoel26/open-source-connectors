// Copyright (c) 2021 Boomi, LP.
package com.boomi.connector.oracledatabase.get;

import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.text.StringEscapeUtils;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.oracledatabase.util.OracleDatabaseConstants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

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
		this.parsedQuery = parse(buildQueryForNClause(query, data), indexMap);
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
	@SuppressWarnings({"java:S3776"})
	public static final String parse(String query, Map<String, Object> paramMap) {
		int length = query.length();
		StringBuilder parsedQuery = new StringBuilder(length);
		boolean inSingleQuote = false;
		boolean inDoubleQuote = false;
		int index = 1;
		paramMap.clear();
		int k = 0;
		for (; k < length; k++) {
			char characterPosition = query.charAt(k);
			if (inSingleQuote) {
				if (characterPosition == '\'') {
					inSingleQuote = false;
				}
			} else if (inDoubleQuote) {
				if (characterPosition == '"') {
					inDoubleQuote = false;
				}
			} else {
				if (characterPosition == '\'') {
					inSingleQuote = true;
				} else if (characterPosition == '"') {
					inDoubleQuote = true;
				} else if (characterPosition == '$' && k + 1 < length
						&& Character.isJavaIdentifierStart(query.charAt(k + 1))) {
					int j = k + 2;
					j = checkIfJavaIdentifier(query, length, j);
					String name = query.substring(k + 1, j);
					characterPosition = '?'; // replace the parameter with a question mark

					k = k + name.length(); // skip past the end if the parameter

					List<Integer> indexList = (List<Integer>) paramMap.get(name.toUpperCase());
					if (indexList == null) {
						indexList = new LinkedList<>();
						paramMap.put(name.toUpperCase(), indexList);
					}
					indexList.add(Integer.valueOf(index));

					index++;
				}
			}
			parsedQuery.append(characterPosition);
		}

		// replace the lists of Integer objects with arrays of ints
		for (Iterator<Map.Entry<String, Object>> itr = paramMap.entrySet().iterator(); itr.hasNext();) {
			Map.Entry<String, Object> entry = itr.next();
			List<?> list = (List<?>) entry.getValue();
			int[] indexes = new int[list.size()];
			int i = 0;
			for (Iterator<?> iterator = list.iterator(); iterator.hasNext();) {
				Integer x = (Integer) iterator.next();
				indexes[i++] = x.intValue();
			}
			entry.setValue(indexes);
		}

		return parsedQuery.toString();
	}

	/**
	 * Gets the keys list.
	 *
	 * @param query the query
	 * @return the keys list
	 */
	@SuppressWarnings({"java:S3776"})
	private List<String> getKeysList(String query) {
		int length = query.length();
		List<String> paramList = new ArrayList<>();
		boolean inSingleQuote = false;
		boolean inDoubleQuote = false;
		for (int i = 0; i < length; i++) {
			char characterPosition = query.charAt(i);
			if (inSingleQuote) {
				if (characterPosition == '\'') {
					inSingleQuote = false;
				}
			} else if (inDoubleQuote) {
				if (characterPosition == '"') {
					inDoubleQuote = false;
				}
			} else {
				if (characterPosition == '\'') {
					inSingleQuote = true;
				} else if (characterPosition == '"') {
					inDoubleQuote = true;
				} else if (characterPosition == '$' && i + 1 < length
						&& Character.isJavaIdentifierStart(query.charAt(i + 1))) {
					int j = i + 2;
					j = checkIfJavaIdentifier(query, length, j);
					String name = query.substring(i + 1, j);
					paramList.add(name);
				}
			}

		}
		return paramList;
	}

	private static int checkIfJavaIdentifier(String query, int length, int j) {
		while (j < length && (Character.isJavaIdentifierPart(query.charAt(j)) || query.charAt(j) == '.')) {
			j++;
		}
		return j;
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
		ObjectMapper mapper = new ObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
		StringBuilder replacing = new StringBuilder();
		try (InputStream is = objdata.getData();) {
			JsonNode json = null;
			if (is.available() == 0 || null == (json = mapper.readTree(is))) {
				throw new ConnectorException("Please check the Input Request!!!");
			}
			String stringUppercase = null;
			if (query.contains("IN(") || query.contains("IN (")) {
				stringUppercase = query.replaceAll("IN\\s+\\(", OPEN_IN);
			} else if (query.contains("in(") || query.contains("in (")) {
				stringUppercase = query.replace("in(", "IN(");
				stringUppercase = stringUppercase.replaceAll("in\\s+\\(", OPEN_IN);
			}
			Iterator<String> keys = getKeysList(query).iterator();
			while (keys.hasNext()) {
				stringUppercase = inClauseQuery(replacing, json, stringUppercase, keys);

			}
			return stringUppercase;

		}
	}

	/**
	 * 
	 * @param replacing
	 * @param json
	 * @param stringUppercase
	 * @param keys
	 * @return
	 */
	private String inClauseQuery(StringBuilder replacing, JsonNode json, String stringUppercase,
			Iterator<String> keys) {
		String key = keys.next();
		if (json.get(key) instanceof ArrayNode) {
			replacing.append(" IN(");
			int arraySize = json.get(key).size();
			for (int j = 0; j < arraySize; j++) {
				replacing.append("\\$" + key);
				if (j < arraySize - 1) {
					replacing.append(COMMA);
				}
			}
			replacing.append(')');
			Pattern pattern = Pattern.compile(" IN\\(\\$" + key + "\\)");
			if (stringUppercase != null) {
				Matcher matcher = pattern.matcher(stringUppercase);
				if (matcher.find()) {
					stringUppercase = matcher.replaceFirst(replacing.toString());
				} else {
					throw new ConnectorException("Invalid IN clause, Please check the IN clause in "
							+ OracleDatabaseConstants.SQL_QUERY);
				}
			}
			replacing.setLength(0);
		}
		return stringUppercase;
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
	 * @see PreparedStatement#setString(int, java.lang.String)
	 */
	public void setString(String name, String value) throws SQLException {
		int[] indexes = getIndexes(name.toUpperCase());
		for (int index : indexes) {
			this.statement.setString(index, value);
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

		for (int index : indexes) {
			JsonNode node = null;
			if (!slaidsIterator.hasNext()) {
				slaidsIterator = array.elements();
			}
			node = slaidsIterator.next();

			this.statement.setString(index, node.toString().replace(BACKSLASH, ""));
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
		for (int index : indexes) {
			JsonNode node = null;
			if (!slaidsIterator.hasNext()) {
				slaidsIterator = array.elements();
			}
			node = slaidsIterator.next();
			this.statement.setString(index, StringEscapeUtils.unescapeJava(node.toString().replace(BACKSLASH, "")));
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
		for (int index : indexes) {
			this.statement.setInt(index, value);
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
		for (int index : indexes) {
			JsonNode node = null;
			if (!slaidsIterator.hasNext()) {
				slaidsIterator = array.elements();
			}
			node = slaidsIterator.next();
			this.statement.setInt(index, Integer.parseInt(node.toString().replace(BACKSLASH, "")));

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
		for (int index : indexes) {
			this.statement.setLong(index, value);
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
		for (int index : indexes) {
			this.statement.setTimestamp(index, value);
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
		for (int index : indexes) {
			this.statement.setDate(index, value);
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
		for (int index : indexes) {
			JsonNode node = null;
			if (!slaidsIterator.hasNext()) {
				slaidsIterator = array.elements();
			}
			node = slaidsIterator.next();
			this.statement.setDate(index, Date.valueOf(node.toString().replace(BACKSLASH, "")));
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
		for (int index : indexes) {
			this.statement.setTime(index, value);
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
		for (int index : indexes) {
			JsonNode node = null;
			if (!slaidsIterator.hasNext()) {
				slaidsIterator = array.elements();
			}
			node = slaidsIterator.next();
			this.statement.setTime(index, Time.valueOf(node.toString().replace(BACKSLASH, "")));
		}
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
		for (int index : indexes) {
			this.statement.setBoolean(index, value);
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
		for (int index : indexes) {
			JsonNode node = null;
			if (!slaidsIterator.hasNext()) {
				slaidsIterator = array.elements();
			}
			node = slaidsIterator.next();
			this.statement.setBoolean(index, Boolean.valueOf(node.toString().replace(BACKSLASH, "")));
		}
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
		for (int index : indexes) {
			JsonNode node = null;
			if (!slaidsIterator.hasNext()) {
				slaidsIterator = array.elements();
			}
			node = slaidsIterator.next();

			this.statement.setLong(index, Long.parseLong(node.toString().replace(BACKSLASH, "")));
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
		for (int index : indexes) {
			JsonNode node = null;
			if (!slaidsIterator.hasNext()) {
				slaidsIterator = array.elements();
			}
			node = slaidsIterator.next();

			this.statement.setFloat(index, Float.parseFloat(node.toString().replace(BACKSLASH, "")));
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
		for (int index : indexes) {
			JsonNode node = null;
			if (!slaidsIterator.hasNext()) {
				slaidsIterator = array.elements();
			}
			node = slaidsIterator.next();

			this.statement.setDouble(index, Double.parseDouble(node.toString().replace(BACKSLASH, "")));
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
		for (int index : indexes) {
			JsonNode node = null;
			if (!slaidsIterator.hasNext()) {
				slaidsIterator = array.elements();
			}
			node = slaidsIterator.next();

			String value = node.toString().replace(BACKSLASH, "");
			try (InputStream stream = new ByteArrayInputStream(value.getBytes());) {
				this.statement.setBlob(index, stream);
			}
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
		for (int index : indexes) {
			this.statement.setFloat(index, value);
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
		for (int index : indexes) {
			this.statement.setDouble(index, value);
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
		for (int index : indexes) {
			try (InputStream stream = new ByteArrayInputStream(value.getBytes());) {
				this.statement.setBlob(index, stream);
			}
		}
	}

	/**
	 * Closes the statement.
	 * 
	 * @throws SQLException if an error occurred
	 * @see PreparedStatement#close()
	 */
	@Override
	public void close() throws SQLException {
		this.statement.close();
	}

	/**
	 * Adds the current set of parameters as a batch entry.
	 * 
	 * @throws SQLException if something went wrong
	 */
	public void addBatch() throws SQLException {
		this.statement.addBatch();
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

}
