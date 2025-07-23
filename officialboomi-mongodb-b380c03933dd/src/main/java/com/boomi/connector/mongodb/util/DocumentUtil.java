// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.mongodb.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.IterableCodecProvider;
import org.bson.codecs.MapCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.api.BasePayload;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.Payload;
import com.boomi.connector.mongodb.TrackedDataWrapper;
import com.boomi.connector.mongodb.constants.MongoDBConstants;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;
import com.mongodb.client.model.Filters;

/**
 * Utils class with helper methods including jsonSchema from Class, get max item in Loong list, min item in Long list etc
 *
 */
public class DocumentUtil {

	private static final Codec<Document> DEFAULT_CODEC = CodecRegistries.
			withUuidRepresentation(CodecRegistries.fromProviders
			(Arrays.asList(new ValueCodecProvider(), new IterableCodecProvider(),
					new BsonValueCodecProvider(), new DocumentCodecProvider(),
					new MapCodecProvider())), UuidRepresentation.STANDARD).get(Document.class);

	private DocumentUtil() {
		throw new IllegalStateException("Utility class");
	}

	/**
	 * Converts given Input stream to string.
	 *
	 * @param inputStream the input stream
	 * @param charset the charset
	 * @return the string
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static String inputStreamToString(InputStream inputStream, Charset charset) throws IOException {
		StringBuilder stringBuilder = new StringBuilder();
		
		if (charset == null) {
			charset = StandardCharsets.UTF_8;
		}
		
		String line = null;
		try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, charset))) {
			while ((line = bufferedReader.readLine()) != null) {
				stringBuilder.append(line);
			}
		}
		return stringBuilder.toString();
	}
	
	/**
	 * This method calculates the batch size.
	 * @param responseLogger
	 * @param batchSizeInput
	 * @param atomMaxBatchSize
	 * @return batchSize
	 */
	public static int fetchSafeBatchSize(Logger responseLogger, Long batchSizeInput, int atomMaxBatchSize) {
		int batchSize = Math.min(batchSizeInput.intValue(), atomMaxBatchSize);
		if (batchSize != batchSizeInput) {
			responseLogger.log(Level.INFO, "Batch size reduced to max batch size in allowed in atom config: {0} ", batchSize);
		}
		return batchSize;
	}
	
	/**
	 * Gets the json schema for a given bean class
	 *
	 * @param clazz the clazz
	 * @return the json schema
	 * @throws JsonProcessingException the json processing exception
	 */
	public static String getJsonSchema(@SuppressWarnings("rawtypes") Class clazz) throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
		JavaType javaType = mapper.getTypeFactory().constructType(clazz);
		JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(mapper);
		JsonSchema schema = schemaGen.generateSchema(javaType);
		return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(schema);
	}
	
	/**
	 * Prepares a list of {@link Document} from given list of {@link TrackedDataWrapper}
	 *
	 * @param dataBatch the data batch
	 * @return the docs from input batch
	 */
	public static List<Document> getDocsFromInputBatch(List<TrackedDataWrapper> dataBatch) {
		List<Document> docList = new ArrayList<>();
		for (int i = 0; i < dataBatch.size(); i++) {
			docList.add(dataBatch.get(i).getDoc());
		}
		return docList;
	}
	
	/**
	 * Returns Time stat with given time unit as String.
	 *
	 * @param timeStat the time stat
	 * @param unit the unit
	 * @return the string
	 */
	public static String timeStatWithUnit(Long timeStat,String unit){
		return new StringBuffer().append(timeStat).append(MongoDBConstants.SINGLE_SPACE).append(unit).toString();
	}
	
	/**
	 * Gets the max item.
	 *
	 * @param longValList the long val list
	 * @return the max item
	 */
	public static Long getMaxItem(List<Long> longValList) {
		Long maxTime = 0L;
		if (!longValList.isEmpty()) {
			if (longValList.size() != 1) {
				Collections.sort(null);
				Collections.reverse(longValList);
			}
			maxTime = longValList.get(0);
		}
		return maxTime;
	}
	
	/**
	 * Gets the min item.
	 *
	 * @param longValList the long val list
	 * @return the min item
	 */
	public static Long getMinItem(List<Long> longValList) {
		Long minTime = 0L;
		if (!longValList.isEmpty()) {
			if (longValList.size() != 1) {
				Collections.sort(null);
			}
			minTime = longValList.get(0);
		}
		return minTime;
	}
	
	/**
	 * Sum.
	 *
	 * @param longValList the long val list
	 * @return the long
	 */
	public static Long sum(List<Long> longValList) {
		Long sum = 0L;
		for (int i = 0; i < longValList.size(); i++) {
			sum += longValList.get(i);
		}
		return sum;
	}
	
	/**
	 * Checks if is blank.
	 *
	 * @param str the str
	 * @return true, if is blank
	 */
	public static boolean isBlank(String str) {
		return null == str || str.length() == 0;
	}

	/**
	 * Converts the Document into Payload
	 *
	 * @param document the document
	 * @return Payload
	 */
	public static Payload toPayLoad(final Document document){
		return new BasePayload() {
			@Override
			public void writeTo(OutputStream out) throws IOException {
				if(document==null){
					throw new ConnectorException("Document is null");
				}
				try (OutputStreamWriter streamWriter = new OutputStreamWriter(out, StandardCharsets.UTF_8);
						JsonWriter jsonWriter = new JsonWriter(streamWriter,
								JsonWriterSettings.builder().outputMode(JsonMode.STRICT).build())) {
					DEFAULT_CODEC.encode(jsonWriter, document, EncoderContext.builder().build());
				}
			}
		};
	}

	/**
	 * Generates a MongoDB query string to filter documents based on their size.
	 *
	 * @param size The maximum size limit for BSON documents.
	 * @return A MongoDB query string for filtering by size.
	 */
	public static String getSizeLimitQuery(long size) {
		return "function() {var docSize = Object.bsonsize(this);return (docSize <=" + size + ")};";
	}

	/**
	 * Parses a string into a long value or returns a default if parsing fails.
	 *
	 * @param value        The string to parse.
	 * @param defaultValue The default value if parsing fails.
	 * @return The parsed long value or the default.
	 */
	public static long parseLongOrDefault(String value, long defaultValue) {
		try {
			return Long.parseLong(value);
		} catch (NumberFormatException ignored) {
			return defaultValue;
		}
	}

	/**
	 * Constructs a MongoDB filter based on the provided AtomConfig and an optional existing filter.
	 *
	 * @param atomConfig The AtomConfig containing configuration properties.
	 * @param bsonFilter An existing BSON filter. Can be null.
	 * @return A BSON filter constructed based on the AtomConfig and the provided filter.
	 */
	public static Bson buildFilterWithMaxDocumentSize(AtomConfig atomConfig, Bson bsonFilter) {
		Bson finalFilter = bsonFilter != null ? bsonFilter : Filters.empty();
		long maxDocumentSize = atomConfig != null ? parseMaxDocumentSize(atomConfig)
				: MongoDBConstants.DEFAULT_MAX_DOCUMENT_SIZE;

		if (maxDocumentSize < 1) {
			return finalFilter;
		}
		String sizeLimitQuery = getSizeLimitQuery(maxDocumentSize);
		return Filters.and(finalFilter, Filters.where(sizeLimitQuery));
	}

	/**
	 * Parses the maximum document size from the provided AtomConfig.
	 *
	 * @param atomConfig the AtomConfig containing the maximum document size property
	 * @return the parsed maximum document size, or the default value if not found or invalid
	 */
	private static long parseMaxDocumentSize(AtomConfig atomConfig) {
		return parseLongOrDefault(atomConfig.getContainerProperty(MongoDBConstants.MAX_DOCUMENT_SIZE_PROPERTY_KEY),
				MongoDBConstants.DEFAULT_MAX_DOCUMENT_SIZE);
	}
}