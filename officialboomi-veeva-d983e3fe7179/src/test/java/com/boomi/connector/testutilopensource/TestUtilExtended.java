// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.testutilopensource;

import com.boomi.connector.api.Connector;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.ConnectorTester;
import com.boomi.connector.testutil.SimpleBrowseContext;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.StreamUtil;

import org.xmlunit.builder.DiffBuilder;
import org.xmlunit.diff.DefaultNodeMatcher;
import org.xmlunit.diff.Diff;
import org.xmlunit.diff.ElementSelectors;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestUtilExtended {

    private static final Logger LOG = LogUtil.getLogger(TestUtilExtended.class);

    private TestUtilExtended() {
        // hide the implicit public constructor
    }

    public static void testBrowseTypes(String testName, Connector connector, OperationType operationType,
            String customOperationType, Map<String, Object> connProps, Map<String, Object> opProps,
            boolean writeExpected) throws IOException {
        ConnectorTester tester = new ConnectorTester(connector);

        String actual;
        SimpleBrowseContext sbc = new SimpleBrowseContext(null, connector, operationType, customOperationType,
                connProps, opProps);
        tester.setBrowseContext(sbc);
        actual = tester.browseTypes();
        compareXML(actual, testName, writeExpected);
    }

    public static void compareXML(String actual, String testName, boolean writeExpected) throws IOException {
        LOG.info(testName);
        if (writeExpected) {
            String expectedDir = "src/test/java/resources/expected/";
            File dir = new File(expectedDir);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            try (OutputStreamWriter writer = new OutputStreamWriter(
                    Files.newOutputStream(Paths.get(expectedDir + testName + ".xml")), StandardCharsets.UTF_8)) {
                writer.write(actual);
                writer.flush();
            }
            return;
        }

        String expected = readFile("src/test/java/resources/expected/" + testName + ".xml");

        Diff myDiffSimilar;
        myDiffSimilar = DiffBuilder.compare(expected).withTest(actual).withNodeMatcher(
                new DefaultNodeMatcher(ElementSelectors.byName)).checkForSimilar().ignoreWhitespace().build();

        if (myDiffSimilar.hasDifferences()) {
            LOG.info("");
            LOG.info(testName);
            LOG.info(myDiffSimilar.toString());
            LOG.log(Level.INFO, "Actual: {0} Expected: {1}", new int[] { actual.length(), expected.length() });
            LOG.info("ACTUAL");
            LOG.info(actual);
            LOG.info("EXPECTED");
            LOG.info(expected);
        }

        StringBuilder resolutionError = new StringBuilder();
        if (actual.contains("\"$ref\"")) {
            resolutionError.append("Definition includes unresolved $ref. ");
        }
        if (actual.contains("\"allOf\"")) {
            resolutionError.append("Definition includes unresolved allOf. ");
        }
        if (actual.contains("\"oneOf\"")) {
            resolutionError.append("Definition includes unresolved oneOf. ");
        }
        if (actual.contains("\"anyOf\"")) {
            resolutionError.append("Definition includes unresolved anyOf. ");
        }
        if (resolutionError.length() > 0) {
            throw new ConnectorException(resolutionError.toString());
        }

        assertFalse(myDiffSimilar.hasDifferences());
        assertEquals(actual.length(), expected.length());
    }

    public static String readFile(String resourcePath) {
        InputStream stream = null;
        try {
            stream = Files.newInputStream(Paths.get(resourcePath));
            return StreamUtil.toString(stream, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new ConnectorException("Error loading resource '" + resourcePath + "'", e);
        } finally {
            IOUtil.closeQuietly(stream);
        }
    }
}