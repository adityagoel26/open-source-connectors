//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb;

import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.JSON_SCHEMA_FIELD_ITEMS;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.JSON_SCHEMA_FIELD_PROPERTIES;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.JSON_SCHEMA_FIELD_TYPE;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.QUERY_ROOT;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.Stack;

import com.boomi.connector.api.Expression;
import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.GroupingExpression;
import com.boomi.connector.api.GroupingOperator;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.api.QueryRequest;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.SimpleExpression;
import com.boomi.connector.api.Sort;
import com.boomi.connector.cosmosdb.action.RetryableQueryOperation;
import com.boomi.connector.cosmosdb.util.CosmosDbConstants;
import com.boomi.connector.cosmosdb.util.CosmosDbConstants.QueryOp;
import com.boomi.connector.exception.CosmosDBConnectorException;
import com.boomi.connector.util.BaseQueryOperation;
import com.boomi.util.CollectionUtil;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * @author Abhijit Mishra
 *
 *         ${tags}
 */
public class CosmosDBQueryOperation extends BaseQueryOperation {

	/** The jsonfactory. */
	private JsonFactory jsonfactory = null;

	/** The record schema for query. */
	private String recordSchemaForQuery = null;

	/**
	 * Instantiates a new Cosmos DB connector for QUERY operation.
	 *
	 * @param conn the conn
	 */
	protected CosmosDBQueryOperation(CosmosDBConnection conn) {
		super(conn);
	}

	/**
	 * This method is used to achieve the QUERY operation of Cosmos DB connector.
	 */
	@Override
	protected void executeQuery(QueryRequest queryRequest, OperationResponse operationResponse) {
		String objectTypeId = this.getContext().getObjectTypeId();
		Map<String, Object> inputConfig = getConnection().prepareInputConfig();
		try {
			FilterData requestData = queryRequest.getFilter();
			QueryFilter filter = requestData.getFilter();
			List<Map.Entry<String, String>> baseQueryTerms = constructQueryTerms(filter);
			if(baseQueryTerms == null) {
				baseQueryTerms = new ArrayList<>();
			}
			checkforSorting(baseQueryTerms, filter);
			prepareProjections(baseQueryTerms,getContext().getSelectedFields());
			RetryableQueryOperation retryableQueryOperation = new RetryableQueryOperation(getConnection(), requestData, objectTypeId, operationResponse, inputConfig,baseQueryTerms);
			retryableQueryOperation.execute();

		} catch (Exception e) {
			ResponseUtil.addExceptionFailure(operationResponse, queryRequest.getFilter(), e);
		}
	}

	/**
	 * This method is used to form Projections / Fields to show in the results
	 */
	private void prepareProjections(List<Entry<String, String>> baseQueryTerms, List<String> projectionFields) {
		if(projectionFields !=null && !projectionFields.isEmpty()) {
			StringBuilder projectionQuery = new StringBuilder("SELECT ");
			String arrayProjection = null;
			for(String projectionField : projectionFields) {
				if(projectionField.contains("/")) {
				if(arrayProjection == null || (!projectionField.substring(0,projectionField.indexOf('/')).equalsIgnoreCase(arrayProjection))) {
					arrayProjection = projectionField.substring(0,projectionField.indexOf('/'));
					projectionQuery.append(QUERY_ROOT).append(projectionField.substring(0,projectionField.indexOf('/'))).append(", ");
				}
				}else {
				projectionQuery.append(QUERY_ROOT).append(projectionField.replace("/", ".")).append(", ");
				}
			}
			baseQueryTerms.add(new AbstractMap.SimpleEntry<String, String>("$projection",projectionQuery.substring(0,projectionQuery.length()-2)+"  FROM root "));
		} else {
			baseQueryTerms.add(new AbstractMap.SimpleEntry<String, String>("$projection","SELECT * FROM root"));
		}
		
	}

	/** 
	 * This method is used for Getting all SORTING parameters and ORDER
	 * parameters which will be added in QUERY POST request body.
	 */
	private void checkforSorting(List<Entry<String, String>> baseQueryTerms, QueryFilter filter)
			throws CosmosDBConnectorException {
		String sortOrder = null;
		List<Sort> sortSpecs = filter.getSort();
		List<String> descendingSort = new ArrayList<>();
		List<String> ascendingSort = new ArrayList<>();
		if (null != sortSpecs && !sortSpecs.isEmpty()) {
			StringBuilder sortQueryString = new StringBuilder(" ORDER BY ");
			for (Sort sortKey : sortSpecs) {
				sortOrder = sortKey.getSortOrder();
				String propertyName = sortKey.getProperty();
				if(propertyName.contains("/")) {
					propertyName = convertArrayObjectFieldType(propertyName);
				}
				switch (sortOrder) {
				case CosmosDbConstants.ASCENDING_ORDER:
					ascendingSort.add(QUERY_ROOT+propertyName);
					break;
				case CosmosDbConstants.DESCENDING_ORDER:
					descendingSort.add(QUERY_ROOT+propertyName);
					break;
				default:
					throw new CosmosDBConnectorException(
							new StringBuffer("Invalid sort order : ").append(sortOrder).toString());
				}
			}
			if(!ascendingSort.isEmpty()) {
				sortQueryString.append(ascendingSort.toString().substring(1, ascendingSort.toString().length()-1))
				.append(CosmosDbConstants.BLANK_SPACE).append(CosmosDbConstants.ASCENDING_ORDER)
				.append(CosmosDbConstants.BLANK_SPACE);
			}
			if (!descendingSort.isEmpty()) {
				if(!ascendingSort.isEmpty()) {
					sortQueryString.append(CosmosDbConstants.COMMA);
				}
				sortQueryString.append(descendingSort.toString().substring(1, descendingSort.toString().length() - 1))
						.append(CosmosDbConstants.BLANK_SPACE).append(CosmosDbConstants.DESCENDING_ORDER)
						.append(CosmosDbConstants.BLANK_SPACE);
			}
			baseQueryTerms.add(new AbstractMap.SimpleEntry<String, String>("$sort",
					sortQueryString.toString()));
		}
	}

	/**
	 * Constructs a list of query filter terms from the given filter, may be
	 * {@code null} or empty.
	 *
	 * @param filter query filter from which to construct the terms
	 *
	 * @return collection of query filter terms for the service
	 * @throws CosmosDBConnectorException
	 * @throws IOException
	 */
	private List<Map.Entry<String, String>> constructQueryTerms(QueryFilter filter)
			throws IllegalStateException, IOException, CosmosDBConnectorException {

		if ((filter == null) || (filter.getExpression() == null)) {
			// no filter given, (this is equivalent to "select all")
			return null;
		}
		List<Map.Entry<String, String>> terms = new ArrayList<>();

		// see if base expression is a single expression or a grouping expression
		Expression baseExpr = filter.getExpression();
		if (baseExpr instanceof SimpleExpression) {
			// base expression is a single simple expression
			String filterValue = constructSimpleExpression(new ArrayList<Expression>(Arrays.asList((SimpleExpression) baseExpr)),"", "");
			terms.add(new AbstractMap.SimpleEntry<String, String>("$filter"," WHERE "+filterValue));

		} else {
			// handle single level of grouped expressions
			GroupingExpression groupExpr = (GroupingExpression) baseExpr;
			String filterValue = constructSimpleExpression(groupExpr.getNestedExpressions(), (groupExpr.getOperator() == GroupingOperator.AND)?"AND":"OR", "");
			terms.add(new AbstractMap.SimpleEntry<String, String>("$filter"," WHERE "+filterValue));
		
		}

		return terms;
	}

	/**
	 * Returns a url query term (key, value pair) constructed from the given
	 * SimpleExpression.
	 *
	 * @param list the simple expression from which to construct the term
	 * @param terms 
	 *
	 * @return url query filter term for the service
	 * @throws CosmosDBConnectorException
	 * @throws IOException
	 */
	private String constructSimpleExpression(List<Expression> list, String groupExp, String queryValue)
			throws IllegalStateException, IOException, CosmosDBConnectorException {

		// parse all the simple expressions in the group
		String value = "";
		for (Expression expression : list) {
			if (expression instanceof GroupingExpression) {
				GroupingExpression groupExpr = (GroupingExpression) expression;
				queryValue+= "(";
				value = constructSimpleExpression(groupExpr.getNestedExpressions(), (groupExpr.getOperator() == GroupingOperator.AND)?"AND":"OR", "");
				queryValue+= value+") "+ ((groupExp==null)?"":groupExp) + " ";
				groupExp = null;
			} else {
			SimpleExpression expr = (SimpleExpression) expression;

			// this is the name of the queried object's property
			String prop = expr.getProperty();

			// translate the operation id into one of our supported operations
			QueryOp queryOp = QueryOp.valueOf(expr.getOperator());

			// we only support 1 argument operations
			if (CollectionUtil.size(expr.getArguments()) != 1) {
				throw new IllegalStateException("Unexpected number of arguments for operation " + queryOp + "; found "
						+ CollectionUtil.size(expr.getArguments()) + ", expected 1");
			}
			String fieldType = getType(prop);
			String param = (fieldType.equalsIgnoreCase("STRING") && !queryOp.equals(QueryOp.IN_LIST)) ? "'" + expr.getArguments().get(0) + "'"
					: expr.getArguments().get(0);
			if(prop.contains("/")) {
				prop = convertArrayObjectFieldType(prop);
			}
			// combine the property name and operation into the query filter value
			if(queryOp.equals(QueryOp.IN_LIST)) {
				queryValue +=QUERY_ROOT+prop.replace("/", ".") + " " + queryOp.getPrefix() + " " + processInputForIncludesQuery(queryOp,param,prop) + " "+((groupExp==null)?"":groupExp) +" ";
			} else {
			queryValue += QUERY_ROOT+prop.replace("/", ".") + " " + queryOp.getPrefix() + " " + param + " "+((groupExp==null)?"":groupExp) +" ";
			}
			}
		}
		if(queryValue.length() - queryValue.lastIndexOf("AND") ==4) {
			queryValue = queryValue.substring(0, queryValue.length() - 5);
		} else if(queryValue.length() - queryValue.lastIndexOf("OR") ==3) {
			queryValue = queryValue.substring(0, queryValue.length() - 4);
		}
		return queryValue;
	}
	
	private String convertArrayObjectFieldType(String property) {
		char[] inputArray = property.toCharArray();
		for(int i=0; i<inputArray.length;i++) {
			String character = String.valueOf(inputArray[i]);
			if(character.matches("[0-9]") && Pattern.compile("/[0-9]/+").matcher(property).find()) {
				inputArray[i-1] = '[';
				inputArray[i+1] = ']';
				i++;
			}
		}
		return new String(inputArray).replace('/', '.').replace("]", "].");
	}
	
	/**
	 * Process input for includes query.
	 *
	 * @param queryOperator   the query operator
	 * @param queryParamValue the query param value
	 * @param fieldName       the field name
	 * @return the list
	 * @throws MongoDBConnectException the mongo DB connect exception
	 */
	private String processInputForIncludesQuery(QueryOp queryOperator, String queryParamValue,
			String fieldName) throws CosmosDBConnectorException {
		List<String> listInput = null;
		String inClauseValue = "'";
			boolean validInputFormat = !StringUtil.isBlank(queryParamValue)
					&& queryParamValue.startsWith(CosmosDbConstants.SQUARE_BRACKET_START)
					&& queryParamValue.endsWith(CosmosDbConstants.SQUARE_BRACKET_END);
			if (validInputFormat) {
				listInput = Arrays.asList(queryParamValue.substring(1, queryParamValue.length() - 1)
						.split(CosmosDbConstants.REGEX_CSV_FORMAT));
				for(String input : listInput) {
					inClauseValue+=input+"','";
				}
				return "("+inClauseValue.substring(0, inClauseValue.length()-2)+")";
			}
			throw new CosmosDBConnectorException(
					new StringBuffer("Invalid param value- ").append(queryParamValue).append("field").append(fieldName)
							.append(" for query operator- ").append(queryOperator).toString());
	}

	/**
	 * Gets the type.
	 *
	 * @param field the field
	 * @return the type
	 * @throws IOException                Signals that an I/O exception has
	 *                                    occurred.
	 * @throws CosmosDBConnectorException the mongo DB connect exception
	 */
	public String getType(String field) throws IOException, CosmosDBConnectorException {
		String[] fieldNames = field.split("/");
		String type = null;
		JsonParser jsonParser = null;
		try {
			jsonParser = getJsonParser();
			type = getFieldType(jsonParser, fieldNames, field);
		} finally {
			if (jsonParser != null) {
				jsonParser.close();
			}
		}
		return type;
	}

	/**
	 * Gets the field type.
	 *
	 * @param jsonParser        the json parser
	 * @param fieldNames        the field names
	 * @param absoluteFieldName the absolute field name
	 * @return the field type
	 * @throws IOException                Signals that an I/O exception has
	 *                                    occurred.
	 * @throws CosmosDBConnectorException the mongo DB connect exception
	 */
	private String getFieldType(JsonParser jsonParser, String[] fieldNames, String absoluteFieldName)
			throws IOException, CosmosDBConnectorException {
		JsonToken currentToken = jsonParser.nextToken();
		String type = null;
		boolean fieldFound = false;
		String currentField = null;
		boolean traversedTillEndToken = false;
		int fieldIndex = 0;
		List<JsonToken> listOfArrayTokens = new ArrayList<>();
		for (; fieldIndex < fieldNames.length; fieldIndex++) {
			String fieldName = fieldNames[fieldIndex];
			fieldFound = false;
			boolean isArrayitem = false;
			boolean isArrayItemFound = false;
			while (currentToken != null) {
				traversedTillEndToken = false;
				if (JsonToken.FIELD_NAME.equals(currentToken) && fieldName.equals(jsonParser.getCurrentName())) {
					fieldFound = true;
					if (fieldIndex < fieldNames.length - 1) {
						traverseFieldSchema(jsonParser, fieldNames[fieldIndex + 1], listOfArrayTokens);
					}
					isArrayitem = isArrayItem(listOfArrayTokens);
					if (isArrayitem) {
						if (fieldIndex == fieldNames.length - 2) {
							isArrayItemFound = true;
						} else {
							fieldIndex++;
							for (; isArrayitem && (fieldIndex < fieldNames.length - 1); fieldIndex++) {
								traverseFieldSchema(jsonParser, fieldNames[fieldIndex + 1], listOfArrayTokens);
								isArrayitem = isArrayItem(listOfArrayTokens);
							}
							if (fieldIndex == fieldNames.length - 1) {
								if (isArrayitem) {
									fieldFound = isArrayitem;
								} else {
									traverseTillEndToken(jsonParser, fieldNames[fieldIndex], JsonToken.FIELD_NAME,
											false);
								}
							} else if (!isArrayitem) {
								fieldIndex++;
							}
						}
					}
					break;
				} else {
					if (JsonToken.FIELD_NAME.equals(currentToken)) {
						currentField = jsonParser.getCurrentName();
					} else if (JsonToken.START_OBJECT.equals(currentToken)) {
						traversedTillEndToken = true;
						traverseTillEndTokenIncluded(jsonParser, currentField, JsonToken.END_OBJECT);
					} else if (JsonToken.START_ARRAY.equals(currentToken)) {
						traversedTillEndToken = true;
						traverseTillEndTokenIncluded(jsonParser, currentField, JsonToken.END_ARRAY);
					}
					currentToken = jsonParser.getCurrentToken();
					if (!traversedTillEndToken || JsonToken.END_OBJECT.equals(currentToken)) {
						currentToken = jsonParser.nextToken();
					}
				}
			}
			if (isArrayItemFound || !StringUtil.isBlank(type)) {
				break;
			} else if (!fieldFound) {
				currentToken = jsonParser.nextToken();
			}
		}
		if (fieldFound) {
			type = getType(jsonParser);
		} else {
			throw new CosmosDBConnectorException(
					new StringBuffer("Error in traversing schema for fetching tye for field - ")
							.append(absoluteFieldName).toString());
		}
		return type;
	}

	/**
	 * Gets the type.
	 *
	 * @param jsonParser the json parser
	 * @return the type
	 * @throws IOException             Signals that an I/O exception has occurred.
	 * @throws MongoDBConnectException the mongo DB connect exception
	 */
	private String getType(JsonParser jsonParser) throws IOException, CosmosDBConnectorException {
		JsonToken currentToken = jsonParser.nextToken();
		String type = null;
		String currentFieldName = jsonParser.getCurrentName();
		while (!(JsonToken.VALUE_STRING.equals(currentToken) && JSON_SCHEMA_FIELD_TYPE.equals(currentFieldName))) {
			currentToken = jsonParser.nextToken();
			currentFieldName = jsonParser.getCurrentName();
		}
		if (!currentToken.equals(JsonToken.VALUE_NULL)) {
			type = jsonParser.getText();
		} else {
			throw new CosmosDBConnectorException("Invalid schema object to fetch type");
		}
		return type;
	}

	/**
	 * Traverse till end token.
	 *
	 * @param jsonParser         the json parser
	 * @param fieldName          the field name
	 * @param endToken           the end token
	 * @param isEndTokenIncluded the is end token included
	 * @return the json parser
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private JsonParser traverseTillEndToken(JsonParser jsonParser, String fieldName, JsonToken endToken,
			boolean isEndTokenIncluded) throws IOException {
		JsonToken currentToken = null;
		currentToken = jsonParser.nextToken();
		while (!fieldName.equals(jsonParser.getCurrentName()) || !endToken.equals(currentToken)) {
			currentToken = jsonParser.nextToken();
		}
		if (isEndTokenIncluded) {
			jsonParser.nextToken();
		}
		return jsonParser;
	}

	/**
	 * Traverse till end token included.
	 *
	 * @param jsonParser the json parser
	 * @param fieldName  the field name
	 * @param endToken   the end token
	 * @return Type casted object based on the field type from schema.
	 * @throws IOException Signals that an I/O exception has occurred.
	 */

	private JsonParser traverseTillEndTokenIncluded(JsonParser jsonParser, String fieldName, JsonToken endToken)
			throws IOException {
		JsonToken currentToken = null;
		currentToken = jsonParser.nextToken();
		if (fieldName != null) {
			while (!fieldName.equals(jsonParser.getCurrentName()) && !endToken.equals(currentToken)) {
				currentToken = jsonParser.nextToken();
			}
		}
		jsonParser.nextToken();
		return jsonParser;
	}

	/**
	 * Traverse field schema.
	 *
	 * @param jsonParser        the json parser
	 * @param childFieldName    the child field name
	 * @param listOfArrayTokens the list of array tokens
	 * @return the json parser
	 * @throws IOException                Signals that an I/O exception has
	 *                                    occurred.
	 * @throws CosmosDBConnectorException the cosmos DB connect exception
	 */
	private JsonParser traverseFieldSchema(JsonParser jsonParser, String childFieldName,
			List<JsonToken> listOfArrayTokens) throws IOException, CosmosDBConnectorException {
		JsonToken currentToken = jsonParser.nextToken();
		boolean isArrayItemFound = false;
		while (null != currentToken) {
			String currentFieldName = jsonParser.getCurrentName();
			if (JsonToken.FIELD_NAME.equals(currentToken) && (JSON_SCHEMA_FIELD_PROPERTIES.equals(currentFieldName)
					|| JSON_SCHEMA_FIELD_ITEMS.equals(currentFieldName))) {
				currentToken = jsonParser.nextToken();
				if (JsonToken.START_OBJECT.equals(currentToken)) {
					currentToken = jsonParser.nextToken();
					break;
				} else if (JsonToken.START_ARRAY.equals(currentToken)) {
					Stack<JsonToken> arrayItemTypeStack = new Stack<>();
					listOfArrayTokens.add(currentToken);
					int arrayItemIndex = 0;
					int fielditemIndex = getArrayItemIndex(childFieldName);
					JsonToken nextToken = jsonParser.nextToken();
					String arrayItemFieldName = null;
					while (!isArrayItemFound && null != nextToken) {
						nextToken = jsonParser.nextToken();
						arrayItemFieldName = jsonParser.getCurrentName();
						if (JsonToken.FIELD_NAME.equals(nextToken)
								&& JSON_SCHEMA_FIELD_TYPE.equals(arrayItemFieldName)) {
							if (arrayItemIndex == fielditemIndex) {
								isArrayItemFound = true;
								break;
							}
						} else if (JsonToken.START_ARRAY.equals(nextToken)
								|| JsonToken.START_OBJECT.equals(nextToken)) {
							arrayItemTypeStack.push(nextToken);
						} else if (!arrayItemTypeStack.isEmpty()) {
							JsonToken lastItemType = arrayItemTypeStack.peek();
							if ((JsonToken.END_ARRAY.equals(nextToken) && JsonToken.START_ARRAY.equals(lastItemType))
									|| (JsonToken.END_OBJECT.equals(nextToken)
											&& JsonToken.START_OBJECT.equals(lastItemType))) {
								arrayItemTypeStack.pop();
								if (arrayItemTypeStack.isEmpty()) {
									arrayItemIndex++;
								}
							}
						}
					}
				}
				if (isArrayItemFound) {
					break;
				}
			}
			currentToken = jsonParser.nextToken();
		}
		if (null == currentToken) {
			throw new CosmosDBConnectorException("Invalid schema object to fetch type");
		}
		return jsonParser;
	}

	/**
	 * Gets the array item index.
	 *
	 * @param childFieldName the child field name
	 * @return the array item index
	 * @throws CosmosDBConnectorException the Cosmos DB connect exception
	 */
	private int getArrayItemIndex(String childFieldName) throws CosmosDBConnectorException {
		int fielditemIndex = -1;
		try {
			fielditemIndex = Integer.parseInt(childFieldName);
		} catch (NumberFormatException ex) {
			throw new CosmosDBConnectorException("Invalid schema object in array to fetch type");
		}
		return fielditemIndex;
	}

	@Override
	public CosmosDBConnection getConnection() {
		return (CosmosDBConnection) super.getConnection();
	}

	/**
	 * Gets the json parser.
	 *
	 * @return the json parser
	 * @throws IOException                Signals that an I/O exception has
	 *                                    occurred.
	 * @throws CosmosDBConnectorException the Cosmos DB connect exception
	 */
	public JsonParser getJsonParser() throws IOException, CosmosDBConnectorException {
		JsonParser jsonParser = getJsonfactory().createParser(getRecordSchemaForQuery());
		initializeParser(jsonParser);
		return jsonParser;
	}

	/**
	 * Gets the jsonfactory.
	 *
	 * @return the jsonfactory
	 */
	public JsonFactory getJsonfactory() {
		if (null == jsonfactory) {
			jsonfactory = new JsonFactory();
		}
		return jsonfactory;
	}

	/**
	 * Gets the record schema for query.
	 *
	 * @return the record schema for query
	 */
	public String getRecordSchemaForQuery() {
		if (StringUtil.isBlank(recordSchemaForQuery)) {
			recordSchemaForQuery = this.getContext().getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT);
		}
		return recordSchemaForQuery;
	}

	/**
	 * Initialize parser.
	 *
	 * @param jsonParser the json parser
	 * @return the json parser
	 * @throws IOException                Signals that an I/O exception has
	 *                                    occurred.
	 * @throws CosmosDBConnectorException the mongo DB connect exception
	 */
	private JsonParser initializeParser(JsonParser jsonParser) throws IOException, CosmosDBConnectorException {
		String currentFieldName = null;
		JsonToken currentToken = null;
		while (!"properties".equals(currentFieldName)) {
			currentFieldName = jsonParser.getCurrentName();
			currentToken = jsonParser.nextToken();
		}
		if (!JsonToken.START_OBJECT.equals(currentToken)) {
			throw new CosmosDBConnectorException("Error while fetching type for field");
		}
		return jsonParser;
	}

	/**
	 * Checks if is array item.
	 *
	 * @param listOfArrayTokens the list of array tokens
	 * @return true, if is array item
	 */
	private boolean isArrayItem(List<JsonToken> listOfArrayTokens) {
		boolean isArrayItem = !listOfArrayTokens.isEmpty()
				&& JsonToken.START_ARRAY.equals(listOfArrayTokens.get(listOfArrayTokens.size() - 1));
		listOfArrayTokens.clear();
		return isArrayItem;
	}

}
