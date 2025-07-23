// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.veeva.operation.json;

import com.boomi.connector.veeva.operation.OperationHelper;

import org.apache.http.entity.ContentType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class JSONCSVStreamerTest {

    private static final ContentType CONTENT_TYPE_CSV = ContentType.create(OperationHelper.TEXT_CSV);

    public static Stream<Arguments> jsonToCsvMappings() {
        String simpleJson = "[{\"file\":\"/test/Text.txt\",\"id\":\"84\",\"rendition_type__v\":\"veeva_eform__v\","
                            + "\"major_version_number__v\":1,\"minor_version_number__v\":0},{\"file\": \"/test/Text"
                            + ".txt\",\"id\":\"83\",\"rendition_type__v\":\"siteconnect_rendition__v\","
                            + "\"major_version_number__v\": 1,\"minor_version_number__v\":0}]";
        String csvFromSimpleJson =
                "\"file\",\"id\",\"rendition_type__v\",\"major_version_number__v\",\"minor_version_number__v\"\n"
                + "\"/test/Text.txt\",\"84\",\"veeva_eform__v\",\"1\",\"0\"\n"
                + "\"/test/Text.txt\",\"83\",\"siteconnect_rendition__v\",\"1\",\"0\"\n";

        String jsonWithCommas =
                "[{\"field1\":\"value1\",\"field2\":\"value2\"},{\"field1\":\"value3,4\",\"field2\":\"value5\"}]";
        String csvFromJsonWithCommas = "\"field1\",\"field2\"\n\"value1\",\"value2\"\n\"value3,4\",\"value5\"\n";

        String jsonWithQuotes = "[{\"field1\":\"val\\\\\\\"ue1\",\"field2\":\"value2\"},{\"field1\":\"value3,4\","
                                + "\"field2\":\"value5\"}]";
        String csvFromJsonWithQuotes = "\"field1\",\"field2\"\n\"val\\\"ue1\",\"value2\"\n\"value3,4\",\"value5\"\n";

        return Stream.of(arguments(simpleJson, csvFromSimpleJson), arguments(jsonWithCommas, csvFromJsonWithCommas),
                arguments(jsonWithQuotes, csvFromJsonWithQuotes));
    }

    @ParameterizedTest
    @MethodSource("jsonToCsvMappings")
    void fromJsonToStreamTest(String inputJson, String expectedCsv) {
        InputStream inputStream = new ByteArrayInputStream(inputJson.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        JSONCSVStreamer streamer = new JSONCSVStreamer();
        streamer.fromJsonToStream(inputStream, outputStream);
        String csvText = outputStream.toString();

        assertEquals(expectedCsv, csvText);
    }

    @Test
    void contentTypeTest() {
        JSONCSVStreamer streamer = new JSONCSVStreamer();
        assertEquals(CONTENT_TYPE_CSV.getMimeType(), streamer.getContentType().getMimeType());
    }
}
