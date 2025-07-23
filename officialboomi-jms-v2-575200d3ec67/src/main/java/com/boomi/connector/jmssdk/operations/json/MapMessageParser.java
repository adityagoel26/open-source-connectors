// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.json;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.jmssdk.util.Utils;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.TempOutputStream;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import javax.jms.JMSException;
import javax.jms.MapMessage;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Parser class for dealing with {@link MapMessage}. It can be used to fill a {@link MapMessage} from the content of an
 * {@link InputStream}, or to build a JSON Payload from the fields in a {@link MapMessage}
 */
public final class MapMessageParser {

    private static final Logger LOG = LogUtil.getLogger(MapMessageParser.class);

    private MapMessageParser() {
    }

    /**
     * Parse the given {@link InputStream} expecting a flat JSON document without nested objects or arrays. Fill the
     * provided {@link MapMessage} with the fields obtained from the JSON document.
     *
     * @param stream  containing a JSON document
     * @param message that will be filled with the fields from the JSON document
     */
    public static void fillMapMessage(InputStream stream, MapMessage message) {
        JsonParser parser = null;
        try {
            parser = JSONUtil.getDefaultJsonFactory().createParser(stream);

            while (shouldContinue(parser.nextToken())) {
                JsonToken currentToken = parser.getCurrentToken();
                if (JsonToken.FIELD_NAME == currentToken) {
                    setFieldIntoMessage(message, parser);
                }
            }
        } catch (IOException e) {
            handleParserError("an error happened parsing the json input document", e);
        } catch (JMSException e) {
            handleParserError("an error happened while building the map message", e);
        } finally {
            IOUtil.closeQuietly(parser);
        }
    }

    private static void setFieldIntoMessage(MapMessage message, JsonParser parser) throws JMSException, IOException {
        String fieldName = parser.getCurrentName();
        JsonToken valueToken = parser.nextToken();

        switch (valueToken) {
            case VALUE_STRING:
                message.setString(fieldName, parser.getValueAsString());
                break;
            case VALUE_NUMBER_FLOAT:
                message.setDouble(fieldName, parser.getValueAsDouble());
                break;
            case VALUE_NUMBER_INT:
                message.setInt(fieldName, parser.getValueAsInt());
                break;
            case VALUE_TRUE:
                message.setBoolean(fieldName, true);
                break;
            case VALUE_FALSE:
                message.setBoolean(fieldName, false);
                break;
            case VALUE_NULL:
                message.setObject(fieldName, null);
                break;
            default:
                // at this point, any other token is unsupported
                handleParserError("unexpected JSON token while parsing the input document: " + valueToken, null);
        }
    }

    private static void handleParserError(String errorMessage, Throwable t) {
        LOG.log(Level.WARNING, errorMessage, t);
        throw new ConnectorException(errorMessage, t);
    }

    private static boolean shouldContinue(JsonToken token) {
        return (token != null) && (token != JsonToken.END_OBJECT);
    }

    /**
     * Build a JSON Payload from the given {@link MapMessage} fields.
     *
     * @param message containing the fields for the JSON
     * @return an {@link InputStream} holding the JSON Payload
     * @throws JMSException if an error happens accessing the fields from the message
     * @throws IOException  if an error happens generating the JSON Payload
     */
    public static InputStream buildJsonPayload(MapMessage message) throws JMSException, IOException {
        Iterable<String> mapKeys = Utils.toIterable(message.getMapNames());

        TempOutputStream outputStream = null;
        JsonGenerator generator = null;
        try {
            outputStream = new TempOutputStream();
            generator = JSONUtil.getDefaultJsonFactory().createGenerator(outputStream);

            generator.writeStartObject();

            for (String key : mapKeys) {
                generator.writeObjectField(key, message.getObject(key));
            }

            generator.writeEndObject();
            generator.flush();

            return outputStream.toInputStream();
        } finally {
            IOUtil.closeQuietly(generator, outputStream);
        }
    }
}
