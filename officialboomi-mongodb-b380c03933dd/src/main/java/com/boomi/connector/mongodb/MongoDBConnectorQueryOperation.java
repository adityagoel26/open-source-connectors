// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb;

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
import com.boomi.connector.mongodb.actions.RetryableQueryOperation;
import com.boomi.connector.mongodb.constants.DataTypes;
import com.boomi.connector.mongodb.constants.MongoDBConstants;
import com.boomi.connector.mongodb.exception.MongoDBConnectException;
import com.boomi.connector.mongodb.util.ProfileUtils;
import com.boomi.connector.mongodb.util.QueryOperationUtil;
import com.boomi.connector.util.BaseQueryOperation;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.boomi.connector.mongodb.constants.MongoDBConstants.FORWARD_SLASH;

/**
 * Implements a logic for performing update operation on mongoDB
 */
@SuppressWarnings("unused")
public class MongoDBConnectorQueryOperation extends BaseQueryOperation {

    /**
     * The number parser.
     */
    private NumberFormat numberParser;

    /**
     * The record schema for query.
     */
    private String recordSchemaForQuery;

    /**
     * The date format.
     */
    private DateFormat dateFormat;

    /**
     * The json parser.
     */
    private JsonParser jsonParser;

    /**
     * The jsonfactory.
     */
    private JsonFactory jsonfactory;

    /**
     * Instantiates a new mongo DB connector query operation.
     *
     * @param conn the conn
     */
    protected MongoDBConnectorQueryOperation(MongoDBConnectorConnection conn) {
        super(conn);
    }

    /**
     * projection means selecting only the necessary data rather than selecting
     * whole of the data of a document. In this method , it Prepare projections for
     * building the query.
     *
     * @param inputConfig the input config
     */
    private void prepareProjections(Map<String, Object> inputConfig) {
        List<String> selectedProj = getContext().getSelectedFields();
        List<String> fields = new ArrayList<>();
        List<Bson> projectionConfig = new ArrayList<>();
        for (String str : selectedProj) {
            if (str.contains(FORWARD_SLASH)) {
                fields.add(str.contains(FORWARD_SLASH) ? str.substring(0,
                        str.indexOf(FORWARD_SLASH)) : str);
            } else {
                fields.add(str);
            }
        }
        if (!fields.contains(MongoDBConstants.ID_FIELD_NAME)) {
            projectionConfig.add(Projections.excludeId());
        }
        projectionConfig.add(Projections.include(fields));
        Bson bsonprojection = Projections.fields(projectionConfig);
        inputConfig.put(MongoDBConstants.QUERY_PROJECTION, bsonprojection);
    }

    /**
     * Execute query based on the filters, projections and sort expression.
     *
     * @param request  the request
     * @param response the response
     */
    @Override
    protected void executeQuery(QueryRequest request, OperationResponse response) {
        String objectTypeId = getContext().getObjectTypeId();
        String objectIdType = null;
        Map<String, Object> inputConfig = getConnection().prepareInputConfig(getContext(), response.getLogger());
        prepareProjections(inputConfig);
        FilterData requestData = request.getFilter();
        QueryFilter filterData = requestData.getFilter();
        Expression filterExpression = filterData.getExpression();
        ProfileUtils profileUtils = null;
        Bson bsonFilter = null;
        String dataType = getContext().getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT);
        try {

            if (dataType.contains("NONE") || (dataType.split(MongoDBConstants.COOKIE).length == 1)) {
                profileUtils = new ProfileUtils(getRecordSchemaForQuery());
                objectIdType = profileUtils.getType(MongoDBConstants.ID_FIELD_NAME);
                bsonFilter = buildBsonFilter(filterExpression, profileUtils);
            } else {
                String[] dataTypearray = dataType.split(MongoDBConstants.COOKIE);
                profileUtils = new ProfileUtils(dataTypearray[1]);
                objectIdType = profileUtils.getType(MongoDBConstants.ID_FIELD_NAME);
                bsonFilter = buildBsonFilter(filterExpression, profileUtils);
            }

            inputConfig.put(MongoDBConstants.QUERY_FILTER, bsonFilter);
            Bson sortSpecs = constructSortSpecs(filterData);
            inputConfig.put(MongoDBConstants.SORT_SPEC, sortSpecs);
            RetryableQueryOperation operation = new RetryableQueryOperation(requestData, getConnection(), objectTypeId,
                    response, inputConfig, objectIdType, getContext());
            operation.execute();
        } catch (IOException | MongoDBConnectException e) {
            getConnection().updateQueryResponse(null, requestData, response, getConnection().processQueryError(e),
                    getContext());
        } catch (Exception e) {
            ResponseUtil.addExceptionFailure(response, request.getFilter(), e);
        } finally {
            getConnection().closeConnection();
        }
    }

    /**
     * Gets the sort spec.
     *
     * @param sort the sort
     * @return the sort spec
     * @throws MongoDBConnectException the mongo DB connect exception
     */
    private Bson getSortSpec(Sort sort) throws MongoDBConnectException {
        Bson sortSpec = null;
        String sortOrder = sort.getSortOrder();
        String propertyName = sort.getProperty();
        switch (sortOrder) {
            case MongoDBConstants.ASCENDING_ORDER:
                sortSpec = Sorts.ascending(Collections.singletonList(propertyName));
                break;
            case MongoDBConstants.DESCENDING_ORDER:
                sortSpec = Sorts.descending(Collections.singletonList(propertyName));
                break;
            default:
                throw new MongoDBConnectException("Invalid sort order : " + sortOrder);
        }
        return sortSpec;
    }

    /**
     * Returns the sort specification that is provided from the platform.
     *
     * @param queryFilter the query filter
     * @return the bson
     * @throws MongoDBConnectException the mongo DB connect exception
     */
    private Bson constructSortSpecs(QueryFilter queryFilter) throws MongoDBConnectException {
        Bson sortSpec = null;
        List<Bson> sortSpecList = new ArrayList<>();
        List<Sort> sortSpecs = queryFilter.getSort();
        if (null != sortSpecs) {
            for (Sort sortKey : sortSpecs) {
                sortSpecList.add(getSortSpec(sortKey));
            }
            if (!sortSpecList.isEmpty()) {
                sortSpec = Sorts.orderBy(sortSpecList);
            }
        }
        return sortSpec;
    }

    /**
     * Process input for querying the IN operator
     *
     * @param queryOperator   the query operator
     * @param queryParamValue the query param value
     * @param fieldName       the field name
     * @return the list
     * @throws MongoDBConnectException the mongo DB connect exception
     */
    private List<String> processInputForIncludesQuery(QueryOperationUtil queryOperator, String queryParamValue,
            String fieldName) throws MongoDBConnectException {
        List<String> listInput = null;
        if (QueryOperationUtil.IN_LIST == queryOperator) {
            boolean validInputFormat = !StringUtil.isBlank(queryParamValue) && queryParamValue.startsWith(
                    MongoDBConstants.SQUARE_BRACKET_OPEN) && queryParamValue.endsWith(
                    MongoDBConstants.SQUARE_BRACKET_CLOSE);
            if (validInputFormat) {
                listInput = Arrays.asList(queryParamValue.substring(1, queryParamValue.length() - 1)
                        .split(MongoDBConstants.REGEX_CSV_FORMAT));
            }
            if (!validInputFormat || (null == listInput) || listInput.isEmpty()) {
                throw new MongoDBConnectException(
                        "Invalid param value- " + queryParamValue + MongoDBConstants.FIELD + fieldName
                                + " for query operator- " + queryOperator);
            }
        }
        return listInput;
    }

    /**
     * Format input for number type.
     *
     * @param input the input
     * @param field the field
     * @return the number
     * @throws MongoDBConnectException the mongo DB connect exception
     */
    private Number formatInputForNumberType(String input, String field) throws MongoDBConnectException {
        Number numberValue = null;
        try {
            numberValue = getNumberParser().parse(input);
        } catch (ParseException e) {
            throw new MongoDBConnectException(
                    "Invalid number format in param value- " + input + MongoDBConstants.FIELD + field);
        }
        return numberValue;
    }

    /**
     * Format input for Decimal type.
     *
     * @param input
     * @param field
     * @return
     * @throws MongoDBConnectException
     */
    private Decimal128 formatInputForDecimal128Type(String input, String field) throws MongoDBConnectException {
        Decimal128 decimalValue = null;
        try {
            decimalValue = new Decimal128(new BigDecimal(input));
        } catch (NumberFormatException e) {
            throw new MongoDBConnectException(
                    "Invalid number format in param value- " + input + MongoDBConstants.FIELD + field);
        }
        return decimalValue;
    }

    /**
     * Format input for boolean type.
     *
     * @param input the input
     * @param field the field
     * @return the boolean
     * @throws MongoDBConnectException the mongo DB connect exception
     */
    private Boolean formatInputForBooleanType(String input, String field) throws MongoDBConnectException {
        Boolean booleanValue = null;
        if (MongoDBConstants.BOOLEAN_TRUE.equalsIgnoreCase(input) || MongoDBConstants.BOOLEAN_FALSE.equalsIgnoreCase(
                input)) {
            booleanValue = Boolean.parseBoolean(input);
        } else {
            throw new MongoDBConnectException(
                    "Invalid Boolean input in param value- " + input + MongoDBConstants.FIELD + field);
        }
        return booleanValue;
    }

    /**
     * Format input for date type.
     *
     * @param input the input
     * @param field the field
     * @return the date
     * @throws MongoDBConnectException the mongo DB connect exception
     */
    private Date formatInputForDateType(String input, String field) throws MongoDBConnectException {
        Date dateValue = null;
        try {
            dateValue = getDateFormat().parse(input);
        } catch (ParseException e) {
            throw new MongoDBConnectException(
                    "Param value- " + input + " for field-" + field + " does not match required date format"
                            + MongoDBConstants.TIMEMASK);
        }
        return dateValue;
    }

    /**
     * Format input for double type.
     *
     * @param input the input
     * @param field the field
     * @return the double
     * @throws MongoDBConnectException the mongo DB connect exception
     */
    private Double formatInputForDoubleType(String input, String field) throws MongoDBConnectException {
        Double doubleValue = null;
        try {
            if (StringUtil.isBlank(input)) {
                throw new MongoDBConnectException("Blank param value not allowed for field-" + field);
            }
            doubleValue = Double.parseDouble(input);
        } catch (NumberFormatException e) {
            throw new MongoDBConnectException(
                    "Invalid Double input in param value- " + input + MongoDBConstants.FIELD + field);
        }
        return doubleValue;
    }

    /**
     * Format input for long type.
     *
     * @param input the input
     * @param field the field
     * @return the long
     * @throws MongoDBConnectException the mongo DB connect exception
     */
    private Long formatInputForLongType(String input, String field) throws MongoDBConnectException {
        Long longValue = null;
        try {
            longValue = Long.parseLong(input);
        } catch (NumberFormatException e) {
            throw new MongoDBConnectException(
                    "Invalid Long input in param value- " + input + MongoDBConstants.FIELD + field);
        }
        return longValue;
    }

    /**
     * Format input for null type.
     *
     * @param input the input
     * @param field the field
     * @return the long
     * @throws MongoDBConnectException the mongo DB connect exception
     */
    private Long formatInputForNullType(String input, String field) throws MongoDBConnectException {
        if (!MongoDBConstants.NULL_STRING.equalsIgnoreCase(input)) {
            throw new MongoDBConnectException("Invalid input in param value- " + input + MongoDBConstants.FIELD + field);
        }
        return null;
    }

    /**
     * Format input for string type.
     *
     * @param input the input
     * @param field the field
     * @return the object
     * @throws MongoDBConnectException the mongo DB connect exception
     */
    private Object formatInputForStringType(String input, String field) throws MongoDBConnectException {
        Object val = null;
        if (field.contains("$oid")) {
            if (ObjectId.isValid(input)) {
                val = new ObjectId(input);
            } else {
                throw new MongoDBConnectException(
                        "Invalid input in param value- " + input + MongoDBConstants.FIELD + field);
            }
        } else {
            val = input;
        }
        return val;
    }

    /**
     * Gets the value from the platform and formats them as per the data type.
     *
     * @param field         the field
     * @param value         the value
     * @param queryOperator the query operator
     * @param profileUtils  the ProfileUtils
     * @return the value
     * @throws MongoDBConnectException the mongo DB connect exception
     */
    public Object getValue(String field, String value, QueryOperationUtil queryOperator, ProfileUtils profileUtils)
            throws MongoDBConnectException {
        Object val = null;
        if (field.contains("/")) {
            val = value;
        }
        try {
            String fieldType = profileUtils.getType(field);
            if (StringUtil.isBlank(fieldType)) {
                throw new MongoDBConnectException(
                        "Error while checking record schema for type of expression field-" + field);
            }
            List<String> processedInputForIncludesQuery = processInputForIncludesQuery(queryOperator, value, fieldType);
            switch (fieldType.toUpperCase()) {
                case DataTypes.INTEGER:
                case DataTypes.NUMBER:
                    val = getIntDataType(field, value, queryOperator, processedInputForIncludesQuery);
                    break;
                case DataTypes.DECIMAL_128:
                    val = getDecimalValue(field, value, queryOperator, processedInputForIncludesQuery);
                    break;
                case DataTypes.STRING:
                    val = getStringValue(field, value, queryOperator, processedInputForIncludesQuery);
                    break;
                case DataTypes.BOOLEAN:
                    val = getBooleanValue(field, value, queryOperator, processedInputForIncludesQuery);
                    break;
                case DataTypes.DATE:
                    val = getDateValue(field, value, queryOperator, processedInputForIncludesQuery);
                    break;
                case DataTypes.DOUBLE:
                    if (QueryOperationUtil.IN_LIST == queryOperator) {
                        val = getDoubleDatatype(field, processedInputForIncludesQuery);
                    } else {
                        val = formatInputForDoubleType(value, field);
                    }
                    break;
                case DataTypes.LONG:
                    if (QueryOperationUtil.IN_LIST == queryOperator) {
                        val = getLongDataType(field, processedInputForIncludesQuery);
                    } else {
                        val = formatInputForLongType(value, field);
                    }
                    break;
                case DataTypes.OBJECT:
                    throw new MongoDBConnectException("Expression field of type object not supported in Boomi");
                case DataTypes.NULL:
                    if (QueryOperationUtil.IN_LIST == queryOperator) {
                        val = getNullDataType(field, processedInputForIncludesQuery);
                    } else {
                        val = formatInputForNullType(value, field);
                    }
                    break;
                default:
                    throw new MongoDBConnectException(
                            "Invalid value type-" + ((null != value) ? value.getClass().getName() : "NULL")
                                    + " provided for expression field-" + field);
            }
        } catch (IOException e) {
            throw new MongoDBConnectException(e);
        }
        return val;
    }

    private Object getDateValue(String field, String value, QueryOperationUtil queryOperator,
            List<String> processedInputForIncludesQuery) throws MongoDBConnectException {
        Object val;
        if (QueryOperationUtil.IN_LIST == queryOperator) {
            val = getDateDataType(field, processedInputForIncludesQuery);
        } else {
            val = formatInputForDateType(value, field);
        }
        return val;
    }

    private Object getBooleanValue(String field, String value, QueryOperationUtil queryOperator,
            List<String> processedInputForIncludesQuery) throws MongoDBConnectException {
        Object val;
        if (QueryOperationUtil.IN_LIST == queryOperator) {
            val = getBooleanDataType(field, processedInputForIncludesQuery);
        } else {
            val = formatInputForBooleanType(value, field);
        }
        return val;
    }

    private Object getStringValue(String field, String value, QueryOperationUtil queryOperator,
            List<String> processedInputForIncludesQuery) throws MongoDBConnectException {
        Object val;
        if (QueryOperationUtil.IN_LIST == queryOperator) {
            val = getStringDatatype(field, processedInputForIncludesQuery);
        } else {
            val = formatInputForStringType(value, field);
        }
        return val;
    }

    private Object getDecimalValue(String field, String value, QueryOperationUtil queryOperator,
            List<String> processedInputForIncludesQuery) throws MongoDBConnectException {
        Object val;
        if (QueryOperationUtil.IN_LIST == queryOperator) {
            val = getDecimalDataType(field, processedInputForIncludesQuery);
        } else {
            val = formatInputForDecimal128Type(value, field);
        }
        return val;
    }

    private Object getIntDataType(String field, String value, QueryOperationUtil queryOperator,
            List<String> processedInputForIncludesQuery) throws MongoDBConnectException {
        Object val;
        if (QueryOperationUtil.IN_LIST == queryOperator) {
            val = getNumberDataType(field, processedInputForIncludesQuery);
        } else {
            val = formatInputForNumberType(value, field);
        }
        return val;
    }

    private Object getNullDataType(String field, List<String> processedInputForIncludesQuery)
            throws MongoDBConnectException {
        Object val;
        List<Long> formattedInput = new ArrayList<>();
        for (String listItem : processedInputForIncludesQuery) {
            formattedInput.add(formatInputForNullType(listItem, field));
        }
        val = formattedInput;
        return val;
    }

    private Object getLongDataType(String field, List<String> processedInputForIncludesQuery)
            throws MongoDBConnectException {
        Object val;
        List<Long> formattedInput = new ArrayList<>();
        for (String listItem : processedInputForIncludesQuery) {
            formattedInput.add(formatInputForLongType(listItem, field));
        }
        val = formattedInput;
        return val;
    }

    private Object getDoubleDatatype(String field, List<String> processedInputForIncludesQuery)
            throws MongoDBConnectException {
        Object val;
        List<Double> formattedInput = new ArrayList<>();
        for (String listItem : processedInputForIncludesQuery) {
            formattedInput.add(formatInputForDoubleType(listItem, field));
        }
        val = formattedInput;
        return val;
    }

    private Object getDateDataType(String field, List<String> processedInputForIncludesQuery)
            throws MongoDBConnectException {
        Object val;
        List<Date> formattedInput = new ArrayList<>();
        for (String listItem : processedInputForIncludesQuery) {
            formattedInput.add(formatInputForDateType(listItem, field));
        }
        val = formattedInput;
        return val;
    }

    private Object getBooleanDataType(String field, List<String> processedInputForIncludesQuery)
            throws MongoDBConnectException {
        Object val;
        List<Boolean> formattedInput = new ArrayList<>();
        for (String listItem : processedInputForIncludesQuery) {
            formattedInput.add(formatInputForBooleanType(listItem, field));
        }
        val = formattedInput;
        return val;
    }

    private Object getStringDatatype(String field, List<String> processedInputForIncludesQuery)
            throws MongoDBConnectException {
        Object val;
        List<Object> formattedInput = new ArrayList<>();
        for (String listItem : processedInputForIncludesQuery) {
            formattedInput.add(formatInputForStringType(listItem, field));
        }
        val = formattedInput;
        return val;
    }

    private Object getDecimalDataType(String field, List<String> processedInputForIncludesQuery)
            throws MongoDBConnectException {
        Object val;
        List<Number> formattedInput = new ArrayList<>();
        for (String listItem : processedInputForIncludesQuery) {
            formattedInput.add(formatInputForDecimal128Type(listItem, field));
        }
        val = formattedInput;
        return val;
    }

    private Object getNumberDataType(String field, List<String> processedInputForIncludesQuery)
            throws MongoDBConnectException {
        Object val;
        List<Number> formattedInput = new ArrayList<>();
        for (String listItem : processedInputForIncludesQuery) {
            formattedInput.add(formatInputForNumberType(listItem, field));
        }
        val = formattedInput;
        return val;
    }

    /**
     * Builds the bson filter.
     *
     * @param expression   the expression
     * @param profileUtils the ProfileUtils
     * @return the bson
     * @throws MongoDBConnectException the mongo DB connect exception
     */
    public Bson buildBsonFilter(Expression expression, ProfileUtils profileUtils) throws MongoDBConnectException {
        Bson queryFilter = null;
        List<Bson> filter = new ArrayList<>();
        if (null != expression) {
            if (expression instanceof SimpleExpression) {
                queryFilter = constructSimpleExpression((SimpleExpression) expression, profileUtils);
            } else {
                GroupingExpression groupExpression = (GroupingExpression) expression;
                GroupingOperator groupingOperator = groupExpression.getOperator();
                for (Expression nestedExpr : groupExpression.getNestedExpressions()) {
                    filter.add(buildBsonFilter(nestedExpr, profileUtils));
                }
                if (groupingOperator != null) {
                    if (groupingOperator == GroupingOperator.AND) {
                        queryFilter = Filters.and(filter);
                    } else if (groupingOperator == GroupingOperator.OR) {
                        queryFilter = Filters.or(filter);
                    }
                }
            }
        }
        return queryFilter;
    }

    /**
     * The method creates filters for the simple expression provided in the field
     * tab of the platform for all the operators.
     *
     * @param expr         the expr
     * @param profileUtils the ProfileUtils
     * @return the bson
     * @throws MongoDBConnectException the mongo DB connect exception
     */
    public Bson constructSimpleExpression(SimpleExpression expr, ProfileUtils profileUtils)
            throws MongoDBConnectException {
        Document doc = null;
        ProfileUtils profileUtils1 = null;
        String jsonSchema = null;
        String propName = expr.getProperty();
        Bson filter = null;
        String modifiedPropName = propName;
        String[] fields = modifiedPropName.split(FORWARD_SLASH);
        for (String dollarString : fields) {
            if (ProfileUtils.MONGO_EXTENDED_JSON_FIELDNAMES.contains(dollarString)) {
                modifiedPropName = modifiedPropName.replace(dollarString + FORWARD_SLASH, StringUtil.EMPTY_STRING)
                        .replace(dollarString, StringUtil.EMPTY_STRING);
            }
        }

        if (modifiedPropName.charAt(0) == '/') {
            modifiedPropName = modifiedPropName.substring(1);
        }
        if (modifiedPropName.charAt(modifiedPropName.length() - 1) == '/') {
            modifiedPropName = modifiedPropName.substring(0, modifiedPropName.length() - 1);
        }

        modifiedPropName = StringUtil.isNotEmpty(modifiedPropName) ? modifiedPropName.replace("/", ".")
                : modifiedPropName;
        List<String> arguments = expr.getArguments();
        String value = expr.getArguments().get(0);
        QueryOperationUtil queryOperationUtil = QueryOperationUtil.valueOf(expr.getOperator());
        Object formattedParamValue = getValue(propName, arguments.get(0), queryOperationUtil, profileUtils);
        if (propName != null) {
            filter = queryCondition(queryOperationUtil, modifiedPropName, formattedParamValue);
        } else {
            throw new MongoDBConnectException("Property name in query expression cannot be NULL" + " cannot be NULL");
        }
        return filter;
    }

    /**
     * Gets the connection.
     *
     * @return the connection
     */
    @Override
    public MongoDBConnectorConnection getConnection() {
        return (MongoDBConnectorConnection) super.getConnection();
    }

    /**
     * Gets the record schema for query.
     *
     * @return the record schema for query
     */
    public String getRecordSchemaForQuery() {
        if (StringUtil.isBlank(recordSchemaForQuery)) {
            recordSchemaForQuery = getContext().getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT);
            String[] dataTypearray = recordSchemaForQuery.split(MongoDBConstants.COOKIE);
            if (dataTypearray.length > 1) {
                String profile = null;
                if ((dataTypearray[1] != null) && !"null".equalsIgnoreCase(dataTypearray[1])) {
                    profile = dataTypearray[1];
                }
                recordSchemaForQuery = profile;
            }
        }
        return recordSchemaForQuery;
    }

    /**
     * Gets the number parser.
     *
     * @return the number parser
     */
    public NumberFormat getNumberParser() {
        if (null == numberParser) {
            numberParser = NumberFormat.getInstance();
        }
        return numberParser;
    }

    /**
     * Gets the date format.
     *
     * @return the date format
     */
    public DateFormat getDateFormat() {
        if (null == dateFormat) {
            dateFormat = new SimpleDateFormat(MongoDBConstants.TIMEMASK);
        }
        return dateFormat;
    }

    public Bson queryCondition(QueryOperationUtil queryOperationUtil, String modifiedPropName,
            Object formattedParamValue) {

        Bson filter = null;
        switch (queryOperationUtil) {
            case EQUALS:
                filter = Filters.eq(modifiedPropName, formattedParamValue);
                break;
            case NOT_EQUALS:
                filter = Filters.not(Filters.eq(modifiedPropName, formattedParamValue));
                break;
            case GREATER_THAN:
                filter = Filters.gt(modifiedPropName, formattedParamValue);
                break;
            case LESS_THAN:
                filter = Filters.lt(modifiedPropName, formattedParamValue);
                break;
            case GREATER_THAN_OR_EQUALS:
                filter = Filters.gte(modifiedPropName, formattedParamValue);
                break;
            case LESS_THAN_OR_EQUALS:
                filter = Filters.lte(modifiedPropName, formattedParamValue);
                break;
            case IN_LIST:
                filter = Filters.in(modifiedPropName, (List) formattedParamValue);
                break;
            default:
                break;
        }
        return filter;
    }
}