// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;

import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.RequestUtil;
import com.boomi.connector.mongodb.constants.DataTypes;
import com.boomi.connector.mongodb.constants.MongoDBConstants;
import com.boomi.connector.mongodb.exception.MongoDBConnectException;
import com.boomi.connector.mongodb.util.JsonSchemaUtil;
import com.boomi.connector.mongodb.util.ProfileUtils;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonFactory;

/**
 * Implements logic to prepare batches for a given objectIdDataRequest.
 *
 */
public class ObjectIdDataBatchWrapper extends InputWrapper {
	
	/** The opr response. */
	OperationResponse oprResponse = null;
	
	/** The number parser. */
	private NumberFormat numberParser = null;
	
	/** The json parser. */
	private JsonParser jsonParser;
	
	/** The jsonfactory. */
	private JsonFactory jsonfactory;

	/** Iterator that iterates over batches. */
	Iterator<List<ObjectIdData>> requestBatchInputItr = new ArrayList<List<ObjectIdData>>().iterator();

	/**
	 * Instantiates a new batch documents object.
	 *
	 * @param request            the request
	 * @param batchSize            the batch size
	 * @param rsponse 
	 * @param atomConfig            the atom config
	 */
	public ObjectIdDataBatchWrapper(Iterable<ObjectIdData> request, int batchSize, OperationResponse rsponse, AtomConfig atomConfig) {
		this.requestBatchInputItr = RequestUtil.pageIterable(request, batchSize, atomConfig).iterator();
		oprResponse = rsponse;
	}

	/**
	 * Checks if next input batch is available.
	 *
	 * @return true, if successful
	 */
	public boolean hasNext() {
		return requestBatchInputItr.hasNext();
	}

	/**
	 * Prepares the next batch of input data to be processed in the operation by
	 * parsing the input.
	 *
	 * @return the list
	 */
	public List<TrackedDataWrapper> next() {
		List<TrackedDataWrapper> list = new ArrayList<>();
		int currentBatchSize = 0;
		Object val = null;
		Exception ex = null;
		List<ObjectIdData> nextBatch = requestBatchInputItr.next();
		for (; currentBatchSize < nextBatch.size(); currentBatchSize++) {
			ObjectIdData input = nextBatch.get(currentBatchSize);
			String inputId = input.getObjectId();
			TrackedDataWrapper data = new TrackedDataWrapper(input, inputId);
			if (!ObjectId.isValid(inputId)) {
				try {
					val = validObjectId(list, val, input, inputId);
				}catch(Exception e)
				{
					ex = e;
					input.getLogger().log(Level.SEVERE,
							new StringBuffer("Error while parsing JSON record to Document for inputRecord: ")
							.append(currentBatchSize+1).toString());
				}finally {
					data = new TrackedDataWrapper(input, null);
					if (null != ex) {
						data.setErrorDetails(null, ex.getMessage());
						getAppErrorRecords().add(data);
					}}
			}
			else {
				list.add(data);
			}
		}
		String logMessage = new StringBuffer("Total documents parsed in ").append("batch :")
				.append(batchCounter+1).append(" are ").append(list.size()).toString();
		oprResponse.getLogger().log(Level.INFO, logMessage);
		batchCounter++;
		return list;
	}

	private Object validObjectId(List<TrackedDataWrapper> list, Object val, ObjectIdData input, String inputId) {
		ProfileUtils profileUtils;
		Document doc = Document.parse(inputId);
		jsonParser = null;
		String jsonSchema = JsonSchemaUtil.createJsonSchema(doc);
		profileUtils = new ProfileUtils(jsonSchema);
		ArrayList<Object> filterList = new ArrayList<>();
		Object properties = doc.get(MongoDBConstants.ID_FIELD_NAME);
		filterList.add(properties);
		if(jsonSchema.contains("$numberLong"))
		{
		if(properties instanceof Long)
		{
		        String longVal = null;
				val = formatInputForLongType(longVal, doc.get("_id").toString());
			}
		TrackedDataWrapper type = new TrackedDataWrapper(input, val);
		list.add(type);
		}else if(jsonSchema.contains("$numberDecimal")) 
		{
			if(properties instanceof Decimal128)
			{
		        String decimal128Val = null;
				val = formatInputForDecimal128Type(decimal128Val, doc.get("_id").toString());
			}
			TrackedDataWrapper type = new TrackedDataWrapper(input, val);
			list.add(type);
		} else if(jsonSchema.contains("$numberDouble")) 
		{
			if(properties instanceof Double)
			{
				String doubleVal = null;
				val = formatInputForDoubleType(doubleVal, doc.get("_id").toString());
			}
			TrackedDataWrapper type = new TrackedDataWrapper(input, val);
			list.add(type);
		}else {
			try {
				String result = profileUtils.getType(MongoDBConstants.ID_FIELD_NAME);
				switch (result.toUpperCase()) {
		        case DataTypes.INTEGER:
		        case DataTypes.NUMBER:
		            val = formatInputForNumberType(result, doc.get("_id").toString());
		            break;
		        case DataTypes.BOOLEAN:
		            val = formatInputForBooleanType(result, doc.get("_id").toString());
		            break;
		        case DataTypes.NULL:
		            val = formatInputForNullType(result,doc.get("_id", "NULL"));
		            break;
		        case DataTypes.OBJECT:
		        	val = formatInputForStringType(result, doc.get("_id").toString());
		        	break;
		        case DataTypes.BINARY_DATA:
		        case DataTypes.STRING:
		        case DataTypes.DATE:
		        case DataTypes.JAVA_SCRIPT:
		        case DataTypes.TIMESTAMP:
		        	val = doc.get("_id").toString();
		            break;
		        default:
		            throw new MongoDBConnectException("Invalid value type-");
				}
				TrackedDataWrapper type = new TrackedDataWrapper(input, val);
				list.add(type);
			}catch (IOException | MongoDBConnectException e) {
				throw new ConnectorException(e);
			}
		}
		return val;
	}
	
	/**
	 * Parses the input json string.
	 *
	 * @param json the json
	 * @return the json parser
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public JsonParser getJsonParser(String json) throws IOException {
		if (null == jsonParser) {
			jsonParser = getJsonfactory().createParser(json);
		}
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
 * Format input for number type.
 *
 * @param input the input
 * @param field the field
 * @return the number
 * @throws ConnectorException the connector exception
 */
private Number formatInputForNumberType(String input, String field) throws ConnectorException  {
	Number numberValue = null;
	try {
		numberValue = getNumberParser().parse(field);
	} catch (ParseException e) {
		throw new ConnectorException(new StringBuffer("Invalid number format in param value- ").append(input)
				.append(MongoDBConstants.FIELD).append(field).toString());
	}
	return numberValue;
}
/**
 * Format input for Decimal type.
 * @param input
 * @param field
 * @return
 * @throws ConnectorException
 */
private Decimal128 formatInputForDecimal128Type(String input, String field) throws ConnectorException {
	Decimal128 decimalValue = null;
	try {
		decimalValue = new Decimal128(new BigDecimal(field));
	} catch (NumberFormatException e) {
		throw new ConnectorException(new StringBuffer("Invalid number format in param value- ").append(input)
				.append(MongoDBConstants.FIELD).append(field).toString());
	}
	return decimalValue;
}

/**
 * Format input for boolean type.
 *
 * @param input the input
 * @param field the field
 * @return the boolean
 * @throws ConnectorException the mongo DB connect exception
 */
private Boolean formatInputForBooleanType(String input, String field) throws ConnectorException {
	Boolean booleanValue = null;
	if (MongoDBConstants.BOOLEAN_TRUE.equalsIgnoreCase(field)
			|| MongoDBConstants.BOOLEAN_FALSE.equalsIgnoreCase(field)) {
		booleanValue = Boolean.parseBoolean(field);
	} else {
		throw new ConnectorException(new StringBuffer("Invalid Boolean input in param value- ").append(input)
				.append(MongoDBConstants.FIELD).append(field).toString());
	}
	return booleanValue;
}

/**
 * Format input for double type.
 *
 * @param input the input
 * @param field the field
 * @return the double
 * @throws ConnectorException the mongo DB connect exception
 */
private Double formatInputForDoubleType(String input, String field) throws ConnectorException {
	Double doubleValue = null;
	try {
		if (StringUtil.isBlank(field)) {
			throw new ConnectorException(
					new StringBuffer("Blank param value not allowed for field-").append(field).toString());
		}
		doubleValue = Double.parseDouble(field);
	} catch (NumberFormatException e) {
		throw new ConnectorException(new StringBuffer("Invalid Double input in param value- ").append(input)
				.append(MongoDBConstants.FIELD).append(field).toString());
	}
	return doubleValue;
}

/**
 * Format input for long type.
 *
 * @param input the input
 * @param field the field
 * @return the long
 * @throws ConnectorException the mongo DB connect exception
 */
private Long formatInputForLongType(String input, String field) throws ConnectorException {
	Long longValue = null;
	try {
		longValue = Long.parseLong(field);
	} catch (NumberFormatException e) {
		throw new ConnectorException(new StringBuffer("Invalid Long input in param value- ").append(input)
				.append(MongoDBConstants.FIELD).append(field).toString());
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
private Long formatInputForNullType(String input, String field) throws ConnectorException {
	if (!MongoDBConstants.NULL_STRING.equalsIgnoreCase(field)) {
		throw new ConnectorException(new StringBuffer("Invalid input in param value- ").append(input)
				.append(MongoDBConstants.FIELD).append(field).toString());
	}
	return null;
}
/**
 * Format input for string type.
 *
 * @param input the input
 * @param field the field
 * @return the object
 * @throws UnsupportedEncodingException 
 * @throws ConnectorException
 */
private Object formatInputForStringType(String input, String field) throws MongoDBConnectException {
	Object val = null;
		if (ObjectId.isValid(field)) {
			val = new ObjectId(field);
		}
		else {
			throw new MongoDBConnectException(new StringBuffer("Invalid input in param value- ").append(input)
					.append(MongoDBConstants.FIELD).append(field).toString());
		}
	return val;
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
}}