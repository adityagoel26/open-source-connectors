// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva.operation.json;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.veeva.operation.OperationHelper;
import com.boomi.util.IOUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.apache.http.entity.ContentType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class JSONCSVStreamer implements JSONStreamer {

    private static final JsonFactory JSON_FACTORY = JSONUtil.getDefaultJsonFactory();
    private static final ContentType CONTENT_TYPE_CSV = ContentType.create(OperationHelper.TEXT_CSV);

    private final List<String> _csvHeader = new ArrayList<>();
    private final StringBuilder _row = new StringBuilder();
    private boolean _isFirstRow = true;

    /**
     * getContext().createTempOutputStream()
     * getContext().tempOutputStreamToInputStream(tempOutputStream)
     * should we allow combining documents with a single header at top?
     * or should we allow an array?
     * need to write to the outputstream
     *
     * @param input  json input stream
     * @param output csv output stream
     */
    @Override
    public void fromJsonToStream(InputStream input, OutputStream output) {
        JsonParser parser = null;
        try {
            parser = JSON_FACTORY.createParser(input);
            parser.nextToken();
            JsonToken element = parser.getCurrentToken();
            if (element != JsonToken.START_ARRAY) {
                throw new ConnectorException("JSON start object expected but got: " + element.name());
            }
            parser.nextToken();
            element = parser.getCurrentToken();
            if (element != JsonToken.START_OBJECT) {
                throw new ConnectorException("JSON start object expected but got: " + element.name());
            }
            int columnNumber = 0;
            _row.setLength(0);
            while (parser.nextToken() != null && element != JsonToken.END_ARRAY) {
                element = parser.getCurrentToken();
                switch (element) {
                    case FIELD_NAME:
                        String fieldName = parser.getCurrentName();
                        if (_isFirstRow) {
                            _csvHeader.add(fieldName);
                        } else {
                            if (!fieldName.contentEquals(_csvHeader.get(columnNumber))) {
                                throw new ConnectorException(
                                        "Expected CSV JSON Element: " + _csvHeader.get(columnNumber) + ", but got: "
                                        + fieldName);
                            }
                        }
                        parser.nextToken();
                        element = parser.getCurrentToken();
                        if (!element.name().startsWith("VALUE_")) {
                            throw new ConnectorException("JSON value expected but got " + element.name());
                        }
                        if (_row.length() > 0) {
                            _row.append(",");
                        }
                        _row.append('"').append(parser.getText()).append('"');
                        columnNumber++;
                        break;
                    case END_OBJECT:
                        //Stream out the row
                        if (_isFirstRow) {
                            _isFirstRow = false;
                            output.write(getCSVHeader().getBytes(StandardCharsets.UTF_8));
                        }
                        _row.append(System.lineSeparator());
                        output.write(_row.toString().getBytes(StandardCharsets.UTF_8));
                        break;
                    case START_OBJECT:
                        columnNumber = 0;
                        _row.setLength(0);
                        break;
                    case END_ARRAY:
                        break;
                    default:
                        throw new ConnectorException("Unexpected JSON token: " + element.name());
                }
                //skip start object
                element = parser.getCurrentToken();
            }
            // Removed commented code which appended \r\n to header & row and wrote to output
        } catch (IOException e) {
            throw new ConnectorException(e);
        } finally {
            IOUtil.closeQuietly(parser, input);
        }
    }

    /**
     * Returns the output content type for this streamer
     *
     * @return content type with MIME type CSV
     */
    @Override
    public ContentType getContentType() {
        return CONTENT_TYPE_CSV;
    }

    private String getCSVHeader() {
        return _csvHeader.stream().map(field -> "\"" + field + "\"").collect(Collectors.joining(","))
               + System.lineSeparator();
    }
}
