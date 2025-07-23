// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.operation.job;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.JsonPayloadUtil;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.ResettableGZIPInputStream;
import com.boomi.util.StringUtil;
import com.boomi.util.TempOutputStream;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * This class processes a response received from a GetQueryResults api call. GetQueryResults api
 * returns results created by a Big Query Job with job type as Query. The response is parsed in a
 * streaming manner. The response contains a "schema" node that will contain the field names.
 * The response further contains a "rows" node that will contain the actual row data. The field
 * names are read from the "schema" node and will be added to every boomi output document.
 *
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class GetQueryResultsProcessor {

    private static final String ERROR_CANNOT_PARSE_RESPONSE_BODY = "could not parse response body";
    private static final String ERROR_NO_RESPONSE_BODY = "there's no response body";

    private static final String NODE_SCHEMA = "schema";
    private static final String NODE_FIELDS = "fields";
    private static final String NODE_FIELD_NAME = "name";

    private static final String NODE_JOB_REFERENCE = "jobReference";
    private static final String NODE_ERRORS = "errors";
    private static final String NODE_MESSAGE = "message";


    private static final String NODE_PAGE_TOKEN = "pageToken";

    private static final String NODE_ROWS = "rows";
    private static final String NODE_F = "f";
    private static final String NODE_V = "v";

    private final Status _status;
    private final Representation _entity;
    private String _pageToken;

    private final List<String> _fieldList = new ArrayList<>();
    private final ObjectMapper MAPPER = JSONUtil.getDefaultObjectMapper();

    public GetQueryResultsProcessor(Response response) {
        _status = response.getStatus();
        _entity = response.getEntity();
    }

    public boolean process(ObjectData data, OperationResponse opResponse) {
        boolean processed = false;

        if (_entity == null) {
            throw new ConnectorException(String.valueOf(_status.getCode()), ERROR_NO_RESPONSE_BODY);
        }

        InputStream rowStream = null;

        JsonParser jp = null;
        InputStream payload = null;
        ResettableGZIPInputStream gzipStream = null;
        try {
            payload = _entity.getStream();
            gzipStream = new ResettableGZIPInputStream(payload);
            jp = JSONUtil.getDefaultJsonFactory().createParser(gzipStream);
            while (jp.nextToken() != JsonToken.END_OBJECT) {
                String fieldName = jp.getCurrentName();

                if (NODE_PAGE_TOKEN.equals(fieldName)) {
                    _pageToken = jp.nextTextValue();
                }
                if (NODE_JOB_REFERENCE.equals(fieldName)) {
                    jp.skipChildren();
                }
                if (NODE_ERRORS.equals(fieldName)) {
                    jp.nextToken();
                    addErrorsResult(opResponse, data, jp);
                }
                if (NODE_SCHEMA.equals(fieldName)) {
                    _fieldList.addAll(parseSchemaNode(jp));

                    // If rows were already present in the document we need to parse them now
                    if(rowStream != null) {
                        JsonParser rowsParser = null;
                        try {
                            rowsParser = JSONUtil.getDefaultJsonFactory().createParser(rowStream);
                            processed = parseRows(rowsParser, data, opResponse);
                        } finally {
                            IOUtil.closeQuietly(rowStream, rowsParser);
                            rowStream = null;
                        }
                    }
                }
                if (NODE_ROWS.equals(fieldName)) {
                    jp.nextToken();
                    if(!_fieldList.isEmpty()) {
                        processed = parseRows(jp, data, opResponse);
                    } else {
                        // If "rows" node is present before "schema" node in the response we need
                        // to save the "rows" node so that it can be processes once we get the "schema" node
                        rowStream = deepCopy(jp);
                    }
                }
            }
        }
        catch (Exception e) {
            throw new ConnectorException(String.valueOf(_status.getCode()), ERROR_CANNOT_PARSE_RESPONSE_BODY, e);
        }
        finally {
            IOUtil.closeQuietly(payload, rowStream, gzipStream, jp);
        }

        return processed;
    }

    private static List<String> parseSchemaNode(JsonParser jp) throws IOException {
        List<String> fieldList = new ArrayList<>();

        while (jp.nextToken() != JsonToken.END_OBJECT) {
            if (NODE_FIELDS.equals(jp.getCurrentName())) {
                jp.nextToken();
                while (jp.nextToken() != JsonToken.END_ARRAY) {
                    while (jp.nextToken() != JsonToken.END_OBJECT) {
                        if (NODE_FIELD_NAME.equals(jp.getCurrentName())) {
                            jp.nextToken();
                            fieldList.add(jp.getValueAsString());
                        }
                    }
                }
            }
        }

        return fieldList;
    }

    private static InputStream deepCopy(JsonParser jp) throws IOException {

        TempOutputStream outputStream = null;
        JsonGenerator gen = null;
        try {
            outputStream = new TempOutputStream();
            gen = JSONUtil.getDefaultJsonFactory().createGenerator(outputStream, JsonEncoding.UTF8);
            //Need to disable this feature to prevent gen.close() from closing the output stream
            gen.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
            gen.copyCurrentStructure(jp);
            gen.flush();
            gen.close();
            return outputStream.toInputStream();
        }
        finally {
            IOUtil.closeQuietly(outputStream, gen);
        }
    }

    public String getNextPageToken() {
        return _pageToken;
    }

    private boolean parseRows(JsonParser jp, ObjectData data, OperationResponse opResponse) throws IOException {
        boolean processed = false;

        while (jp.nextToken() != JsonToken.END_ARRAY) {
            if (NODE_F.equals(jp.getCurrentName())) {
                jp.nextToken();
                JsonNode row = parseRow(jp);
                ResponseUtil.addPartialSuccess(opResponse, data, String.valueOf(_status.getCode()),
                        JsonPayloadUtil.toPayload(row));
                processed = true;
            }
        }
        return processed;
    }

    private JsonNode parseRow(JsonParser jp) throws IOException{

        ObjectNode row = JSONUtil.newObjectNode();
        int index = 0;
        while (jp.nextToken() != JsonToken.END_ARRAY) {
            while (jp.nextToken() != JsonToken.END_OBJECT) {
                if (NODE_V.equals(jp.getCurrentName())) {
                    row.put(_fieldList.get(index), jp.nextTextValue());
                }
            }
            index++;
        }
        return row;
    }

    /**
     * Parses an "errors" if received in the response. The "errors" node can be received for a 200(OK)
     * response. The errors node indicates errors/warnings encountered during the running of a job.
     * @param opResponse
     * @param data
     * @param jp
     * @throws IOException
     */
    private void addErrorsResult(OperationResponse opResponse, ObjectData data, JsonParser jp)
            throws IOException {
        JsonNode errors = MAPPER.readTree(jp);
        logError(opResponse.getLogger(), errors);
        opResponse.addPartialResult(data, OperationStatus.APPLICATION_ERROR, Integer.toString(Status.SUCCESS_ACCEPTED.getCode()),
                buildErrorMessage(errors), JsonPayloadUtil.toPayload(errors));
    }

    /**
     * Returns the message present in the last error present in errors array node.
     * The final message includes the number of errors that caused the process to stop.
     * @param errors
     * @return
     */
    private static String buildErrorMessage(JsonNode errors) {
        String message = errors.path(errors.size() - 1).path(NODE_MESSAGE).asText();
        return StringUtil.defaultIfEmpty(message, Status.SUCCESS_ACCEPTED.getDescription());
    }

    private static void logError(Logger log, JsonNode errors) {
        if (log != null) {
            log.warning("Error/Warning node received" + errors);
        }
    }
}
