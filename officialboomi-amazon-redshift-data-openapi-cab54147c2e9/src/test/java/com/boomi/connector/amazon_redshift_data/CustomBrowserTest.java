// Copyright (c) 2022 Boomi, Inc.

package com.boomi.connector.amazon_redshift_data;

import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.openapi.OpenAPIBrowser;
import com.boomi.connector.testutil.SimpleAtomConfig;
import com.boomi.connector.testutil.SimpleBrowseContext;
import com.boomi.util.LogUtil;
import org.junit.Test;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CustomBrowserTest {

    private static final Logger LOG = LogUtil.getLogger(CustomBrowserTest.class);
    private static final String SPEC = "custom-specification-amazon_redshift_data.yaml";
    private final Map<String, Object> connProps;

    int endPointCount = 0;
    int errorCount = 0;
    int stackoverflowCount = 0;
    int otherErrorCount = 0;
    ArrayList<String> erroringOperationIds = new ArrayList<>();

    final String[] httpMethods = {
            "GET",
            "POST",
            "PUT",
            "DELETE",
            "PATCH",
            "HEAD",
            "OPTIONS",
            "TRACE"
    };

    public CustomBrowserTest() {
        connProps = new HashMap<>();
        connProps.put("spec", SPEC);
    }

    @Test(expected = Test.None.class)
    public void testTypes() {
        CustomConnector connector = new CustomConnector();

        for (String httpMethod : httpMethods) {
            SimpleBrowseContext browseContext = new SimpleBrowseContext(
                    new SimpleAtomConfig(),
                    connector,
                    null,
                    httpMethod,
                    connProps,
                    null
            );

            OpenAPIBrowser browser = (OpenAPIBrowser) connector.createBrowser(browseContext);
            try {
                browser.getObjectTypes();
            } catch (Exception e) {
                if (e.getMessage() == null || !e.getMessage().contains("no types available")) {
                    throw e;
                }
            }
        }
    }

    @Test(expected = Test.None.class)
    public void testProfiles() {
        for (String httpMethod : httpMethods) {
            browseHttpMethod(httpMethod);
        }

        String message = String.format("%d out of %d endpoints failed.", errorCount, endPointCount);
        LOG.log(Level.INFO, message);
        message = String.format("%d of those are stackoverflow errors.", stackoverflowCount);
        LOG.log(Level.INFO, message);
        message = String.format("%d of those are other errors.", otherErrorCount);
        LOG.log(Level.INFO, message);

        LOG.log(Level.INFO, "List of erroring operation ids:");
        for (String erroringOperationId : erroringOperationIds) {
            LOG.log(Level.INFO, erroringOperationId);
        }
    }

    private void browseHttpMethod(String httpMethod) {
        CustomConnector connector = new CustomConnector();
        SimpleBrowseContext browseContext = new SimpleBrowseContext(
                new SimpleAtomConfig(),
                connector,
                null,
                httpMethod,
                connProps,
                null
        );

        OpenAPIBrowser browser = (OpenAPIBrowser) connector.createBrowser(browseContext);
        ObjectTypes objectTypes = new ObjectTypes();
        try {
            objectTypes = browser.getObjectTypes();
        } catch (Exception e) {
            if (e.getMessage() == null || !e.getMessage().contains("no types available")) {
                throw e;
            }
        }
        for (ObjectType objectType : objectTypes.getTypes()) {
            endPointCount++;
            String path = objectType.getId();
            String operationId = objectType.getLabel();
            Set<ObjectDefinitionRole> roles = new HashSet<>();
            roles.add(ObjectDefinitionRole.INPUT);
            roles.add(ObjectDefinitionRole.OUTPUT);

            try {
                browser.getObjectDefinitions(path, roles);
            } catch (StackOverflowError e) {
                String message = String.format("Stackoverflow error for operationId %s", operationId);
                LOG.log(Level.WARNING, message);
                erroringOperationIds.add(operationId);
                errorCount++;
                stackoverflowCount++;
            } catch (Exception e) {
                String message = String.format(
                        "Browser failing for path: %s, http method: %s, operation id: %s %n error: %s",
                        path, httpMethod, operationId, e.getMessage());
                LOG.log(Level.WARNING, message);
                erroringOperationIds.add(operationId);
                errorCount++;
                otherErrorCount++;
            }
        }
    }
}
