package com.boomi.connector.odataclient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import com.boomi.connector.api.ConnectorException;
import com.boomi.util.IOUtil;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class ODataParseUtil {
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
	private SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss.SSSZZ");
	int _depth=0;
	JsonGenerator _jsonGenerator = null;
	

	/** 
	 * Parse an inbound document payload
	 * Convert JSON dates, times etc to OData formats
	 * Extract keys and return a formated URL predicate for use in edit and GET urls.
	 * If only and all child keys mapped we will just do a link with 
	 * 			 Category: {__metadata: {uri: "/Categories(0)"}}
	 * 			<parent nav property>: {__metadata: {uri: "/<child entityset>(0)"}}
	 * @param is Input stream of JSON
	 * @param os Output stream of parsed JSON. For GET and DELETE operations, there will be no output stream and only predicate keys are returned
	 * @param inputCookie Contains the property metadata for properties, keys and navigation properties
	 * @return the predicate key for building the URL
	 * @throws JsonParseException
	 * @throws IOException
	 * @throws ParseException 
	 */
	public String parseBoomiToOData(InputStream is, OutputStream os, OperationCookie inputCookie) throws JsonParseException, IOException, ParseException {
		StringBuilder predicateKeys = new StringBuilder();
		StringBuilder navigationPropertyPredicateKeys = new StringBuilder();
		String firstKeyValue = null;
		String firstChildKeyValue = null;
		long numberKeys=0;
		long numberChildKeys=0;
		StringBuilder pathPointer = new StringBuilder();
		JsonFactory JSON_FACTORY = new JsonFactory();
		JsonParser parser = null;
		String lastValue = "";
		boolean inLinkElement = false;
		boolean hasChildProperties = false;
		// open the parser and generator as managed resources that are guaranteed to be closed
		try {
			parser = JSON_FACTORY.createParser(is);
			if (os!=null)
			{
				_jsonGenerator = JSON_FACTORY.createGenerator(os);	
				_jsonGenerator.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
			}
			while (parser.nextToken() != null) {
				JsonToken element = parser.getCurrentToken();
				if (element == JsonToken.END_ARRAY) {
					popElement(pathPointer);
					writeEndArray();
				}
				if (element == JsonToken.START_OBJECT) //Note we only hit this one at the root, after that we only find startobjects inside field_name elements below
					writeStartObject();

				if (element == JsonToken.FIELD_NAME) {
					String currentName = parser.getCurrentName();
					pushElement(pathPointer, currentName);
					if (currentName.contentEquals(ODataConstants.CHILD_KEYS_ELEMENT))
					{
						inLinkElement=true;
						numberChildKeys=0;
					}
					// grab the value token
					element = parser.nextToken();
					// write the current field name but not if we are writing a link
					if (!inLinkElement)
					{
						writeFieldName(currentName);
						if (element == JsonToken.START_OBJECT)
						{
							writeStartObject();
						}
						else if (element == JsonToken.START_ARRAY)
						{
							writeStartArray();							
						}
					}
					if (element.name().startsWith(ODataConstants.VALUE)) {
						lastValue=pathPointer.toString();
						String edmType = inputCookie.getEdmType(pathPointer.toString());
						if (edmType!=null)
						{
							if (inputCookie.isKey(pathPointer.toString()) && pathPointer.lastIndexOf("/")==0) //Top level key,
							{
								numberKeys++;
								String odataValue=ODataEdmType.boomiValuetoODataPredicate(parser.getValueAsString(), edmType);
								if (predicateKeys.length()>0)
									predicateKeys.append(",");
								else 
									firstKeyValue = odataValue;
								predicateKeys.append(pathPointer.toString().substring(1)+"="+odataValue);							
							}
															
							if (inLinkElement) //Child key
							{
								numberChildKeys++;
								String childOdataValue=ODataEdmType.boomiValuetoODataPredicate(parser.getValueAsString(), edmType);
								if (navigationPropertyPredicateKeys.length()>0)
									navigationPropertyPredicateKeys.append(",");
								else 
									firstChildKeyValue = childOdataValue;
								String propertyName=pathPointer.toString();
								propertyName=propertyName.substring(propertyName.lastIndexOf("/")+1);
								navigationPropertyPredicateKeys.append(propertyName+"="+childOdataValue);							
							} else {
								String value = parser.getValueAsString();
								if (value==null)
									writeNull();
								else
									BoomiToOdataType(inputCookie, pathPointer, parser, element, edmType, value);
							}
						} else {
							throw new ConnectorException("EDM Type not found in cookie for: " + pathPointer.toString());
						}

						popElement(pathPointer);
					}
				}
				if (element == JsonToken.END_OBJECT && !parser.getParsingContext().inRoot())
				{
					if (!inLinkElement)
						writeEndObject();
					//TODO We will run this before we write a write the first field name that is outside of the last Navigation Property
					else
					{
						popElement(pathPointer);
						inLinkElement=false;
						String childPredicate=null;
						if (numberChildKeys==1)
							childPredicate=firstChildKeyValue;
						else if (numberChildKeys>1)
							childPredicate=navigationPropertyPredicateKeys.toString();
						if (childPredicate==null)
							throw new ConnectorException("Linking requires navigation property keys to be set: " + pathPointer.toString());
						writeFieldName("__metadata");
						writeStartObject();
						if (_jsonGenerator!=null)
							_jsonGenerator.writeStringField("uri", "/" + inputCookie.getEntitySetName(pathPointer.toString())+"("+childPredicate+")");
						writeEndObject();
					}
					popElement(pathPointer);
				}
			}
			// flush the generator
			flush();
		} catch (IOException e) {
			throw new ConnectorException(e.toString() + " " + pathPointer.toString() + ":" + lastValue);
		} finally {
			IOUtil.closeQuietly(parser);
			IOUtil.closeQuietly(is);
			IOUtil.closeQuietly(_jsonGenerator);
		}
		String predicate;
		if (numberKeys==1)
			predicate=firstKeyValue;
		else
			predicate=predicateKeys.toString();
		if (numberKeys==0)
			return "";
		return "(" + predicate + ")";
	}

	private void BoomiToOdataType(OperationCookie inputCookie, StringBuilder pathPointer, JsonParser parser, JsonToken element, String edmType, String value) throws IOException, ParseException {
		switch (edmType)
		{
			case "Single":
			case "Double":
				writeString(String.format("%f", parser.getValueAsDouble()));
				break;
			case "Decimal":
				String fmt = "%f";
				int scale = inputCookie.getScale(pathPointer.toString());
				if (scale>0)
					fmt="%."+scale+"f";
				String dValue = String.format(fmt, parser.getValueAsDouble());
				writeString(dValue);
				break;
			case "DateTimeOffset":
			case "DateTime":
				//"LastChangeDate": "/Date(1588377600000)/",
				dateFormat.setTimeZone(TimeZone.getTimeZone(ODataConstants.UTC));
				value = String.format("/Date(%d)/", dateFormat.parse(value).getTime());
				writeString(value);
				break;
			case "Time":
				//"LastChangeTime": "PT14H27M30S",
				timeFormat.setTimeZone(TimeZone.getTimeZone(ODataConstants.UTC));
				Date dt = timeFormat.parse(value);
				value = String.format("PT%02dH%02dM%02dS", dt.getHours(), dt.getMinutes(), dt.getSeconds());
				writeString(value);
				break;
			default:
				writeJsonTokenToGenerator(element, parser);
				break;
		}
	}

	/**
	 * Parse the inbound document, remove any root "d" object and convert OData date, time, decimal types to boomi/json
	 * TODO parser will leave blank arrays of primitives like [1,2,3]
	 * @param is Input stream of JSON
	 * @param os Output stream of parsed JSON. OS can be null if all we are doing is parsing for an etag
	 * @param operationCookie Contains the property metadata for properties, keys and navigation properties
	 * @return any ETag value found in the response payload
	 * @throws JsonParseException
	 * @throws IOException
	 */
	public String parseODataToBoomi(InputStream is, OutputStream os, OperationCookie operationCookie) {
		StringBuilder pathPointer = new StringBuilder();
		JsonFactory JSON_FACTORY = new JsonFactory();
		// open the parser and generator as managed resources that are guaranteed to be
		// closed
		JsonParser parser = null;
		String lastValue="";
		String eTag = null;
		try {
			parser = JSON_FACTORY.createParser(is);
			if (os!=null)
			{
				_jsonGenerator = JSON_FACTORY.createGenerator(os);	
				_jsonGenerator.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
			}
			while (parser.nextToken() != null) {
				JsonToken element = parser.getCurrentToken();
				if (element == JsonToken.END_ARRAY) {
					popElement(pathPointer);
					writeEndArray();
				}
				if (element == JsonToken.START_OBJECT)
					writeStartObject();

				if (element == JsonToken.FIELD_NAME) {
					String currentName = parser.getCurrentName();
					pushElement(pathPointer, currentName);
					// grab the value token
					element = parser.nextToken();
					if (!"/d".contentEquals(pathPointer)) {
						// write the current field name
						writeFieldName(currentName);

						if (element == JsonToken.START_OBJECT)
							writeStartObject();
						else if (element == JsonToken.START_ARRAY)
							writeStartArray();
						else if (element.name().startsWith(ODataConstants.VALUE)) {
							String cookieKey=pathPointer.toString();
							lastValue = parser.getValueAsString();
							if ("ETag".contentEquals(currentName))
								eTag = lastValue;
							if (cookieKey.startsWith("/d/"))
								cookieKey=cookieKey.substring(2);
							String edmType = operationCookie.getEdmType(cookieKey);
							if (edmType!=null)
							{
								String value = lastValue;
								if (value==null)
									writeNull();
								else
									OdatatoBoomiType(parser, element, edmType, value);
							} else {
								writeJsonTokenToGenerator(element, parser);
							}
							popElement(pathPointer);
						}

					}
				}

				if (element == JsonToken.END_OBJECT && !parser.getParsingContext().inRoot()) // We whacked /d so ignore the last pop
				{
					writeEndObject();
					popElement(pathPointer);
				}
			}
			// flush the generator
			flush();
		} catch (IOException e) {
			throw new ConnectorException(e.toString() + " " + pathPointer.toString() + ":" + lastValue);
		} finally {
			IOUtil.closeQuietly(is);
			IOUtil.closeQuietly(_jsonGenerator);
			IOUtil.closeQuietly(parser);
		}
		return eTag; //TODO this only returns a single Etag, assumes that the response only includes a single entitytype
	}

	private void OdatatoBoomiType(JsonParser parser, JsonToken element, String edmType, String value) throws IOException {
		switch (edmType)
		{
			case "Single":
			case "Double":
			case "Decimal":
				double number = Double.parseDouble(value);
				writeNumber(number);
				break;
			case "DateTimeOffset":
				//"LastChangeDate": "/Date(1588377600000+0000)/",
				boolean isMinus=false;
				int offsetPos = value.indexOf("+");
				if (offsetPos==-1)
				{
					offsetPos = value.indexOf("-");
					if (offsetPos>0)
						isMinus = true;
					else
						throw new ConnectorException("Invalid DateTimeOffset value: " + value);
				}
				Long base=Long.parseLong(value.substring(value.indexOf("(")+1, offsetPos));
				Long offset = Long.parseLong(value.substring(offsetPos+1, value.lastIndexOf(")")));
				if (isMinus)
					base-=offset;
				else
					base+=offset;

				value =dateFormat.format(new Date(base));
				writeString(value);
				break;
			case "DateTime":
				//"LastChangeDate": "/Date(1588377600000)/",
				value = value.substring(value.indexOf("(")+1, value.lastIndexOf(")"));
				dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
				value =dateFormat.format(new Date(Long.parseLong(value)));
				writeString(value);
				break;
			case "Time":
				//"LastChangeTime": "PT14H27M30S",
				int hours = Integer.parseInt(value.substring(2,4));
				int minutes = Integer.parseInt(value.substring(5,7));
				int seconds = Integer.parseInt(value.substring(8,10));
				Date dt = new Date();
				dt.setHours(hours);
				dt.setMinutes(minutes);
				dt.setSeconds(seconds);
				timeFormat.setTimeZone(TimeZone.getTimeZone(ODataConstants.UTC));
				writeString(timeFormat.format(dt));
				break;
			default:
				writeJsonTokenToGenerator(element, parser);
				break;
			}
	}

	public static String parseBoomiToFunctionImportURL(InputStream is, OperationCookie inputCookie) {
		StringBuilder queryParameters = new StringBuilder();
		JsonFactory JSON_FACTORY = new JsonFactory();
		JsonParser parser = null;
		String currentName = "";
		// open the parser and generator as managed resources that are guaranteed to be closed
		try {
			parser = JSON_FACTORY.createParser(is);
			while (parser.nextToken() != null) {
				JsonToken element = parser.getCurrentToken();

				if (element == JsonToken.FIELD_NAME) {
					currentName = parser.getCurrentName();
					// grab the value token
					element = parser.nextToken();
					// write the current field name

					if (element.name().startsWith(ODataConstants.VALUE)) {
						String edmType = inputCookie.getEdmType("/"+currentName);
						if (edmType!=null)
						{
							String odataValue=ODataEdmType.boomiValuetoODataPredicate(parser.getValueAsString(), edmType);
							if (queryParameters.length()>0)
								queryParameters.append("&");
							queryParameters.append(currentName+"="+odataValue);							
						}
					}
				}
			}
			// flush the generator
		} catch (IOException e) {
			throw new ConnectorException(e.toString() + " " + currentName);
		} finally {
			IOUtil.closeQuietly(parser);
			IOUtil.closeQuietly(is);
		}
		return queryParameters.toString();
	}

	private void pushElement(StringBuilder pathPointer, String name) {
		this._depth++;
		pathPointer.append("/");
		pathPointer.append(name);
	}

	private void popElement(StringBuilder pathPointer) {
		_depth--;
		int lastPos = pathPointer.lastIndexOf("/");
		if (lastPos > -1)
			pathPointer.setLength(lastPos);
	}

	private void writeJsonTokenToGenerator(JsonToken element, JsonParser parser) throws IOException
	{
		if (_jsonGenerator!=null)
		{
			if (parser.getValueAsString()==null)
				_jsonGenerator.writeNull();
			else
			{
				switch (element)
				{
				case VALUE_STRING:
					_jsonGenerator.writeString(parser.getValueAsString());
					break;
				case VALUE_TRUE:
					_jsonGenerator.writeBoolean(true);
					break;
				case VALUE_FALSE:
					_jsonGenerator.writeBoolean(false);
					break;
				case VALUE_NUMBER_INT:
					_jsonGenerator.writeNumber(parser.getValueAsLong());
					break;
				case VALUE_NUMBER_FLOAT:
					_jsonGenerator.writeNumber(parser.getValueAsDouble());
					break;
				case VALUE_NULL:
					_jsonGenerator.writeNull();
					break;
				default:
					throw new ConnectorException("Unhandled JSON Value Type: " + element.name());								
				}
			}
		}
	}
	
	private void writeNull() throws IOException
	{
		if (_jsonGenerator!=null)
			_jsonGenerator.writeNull();
	}
	
	private void writeFieldName(String fieldName) throws IOException
	{
		if (_jsonGenerator!=null)
			_jsonGenerator.writeFieldName(fieldName);
	}
	
	private void writeNumber(double number) throws IOException
	{
		if (_jsonGenerator!=null)
			_jsonGenerator.writeNumber(number);
	}
	
	private void writeString(String string) throws IOException
	{
		if (_jsonGenerator!=null)
			_jsonGenerator.writeString(string);
	}
	
	private void flush() throws IOException
	{
		if (_jsonGenerator!=null)
			_jsonGenerator.flush();
	}
	
	private void writeStartObject() throws IOException
	{
		if (_jsonGenerator!=null)
			_jsonGenerator.writeStartObject();
	}
	
	private void writeEndObject() throws IOException
	{
		if (_jsonGenerator!=null)
			_jsonGenerator.writeEndObject();
	}
	
	private void writeStartArray() throws IOException
	{
		if (_jsonGenerator!=null)
			_jsonGenerator.writeStartArray();
	}
	
	private void writeEndArray() throws IOException
	{
		if (_jsonGenerator!=null)
			_jsonGenerator.writeEndArray();
	}
}
