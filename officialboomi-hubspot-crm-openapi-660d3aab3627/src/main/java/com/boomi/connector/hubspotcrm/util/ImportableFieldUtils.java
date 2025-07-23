// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.hubspotcrm.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.util.Args;
import com.boomi.util.ClassUtil;

import java.io.IOException;
import java.io.InputStream;

/**
 * Utility class that provides methods to get predefined fields used in data import operations.
 * These fields are configured with allowed values, display types, and other properties.
 */
public class ImportableFieldUtils {

    private static final String RESOURCE_NOT_FOUND = "Resource not found: ";

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private ImportableFieldUtils() {
        throw new AssertionError("ImportableFieldUtils is a utility class and should not be instantiated");
    }

    /**
     * Retrieves importable fields from a specified resource and adds them to the provided ObjectDefinitions.
     * <p>
     * This method loads importable fields from the resource specified by the browseFieldPath,
     * creates an ImportableFields object, and adds the field values to the given ObjectDefinitions.
     * It uses a try-with-resources statement to ensure proper handling of the InputStream.
     *
     * @param objectDefinitions The ObjectDefinitions to which the importable fields will be added.
     * @param browseFieldPath   The path to the resource containing the importable fields.
     * @throws ConnectorException if the resource cannot be found or if an IOException occurs while reading the
     *                            resource.
     *                            The exception message will include the path of the resource that could not be found
     *                            or read.
     */
    public static void getImportableFields(ObjectDefinitions objectDefinitions, String browseFieldPath) {
        try (InputStream inputStream = Args.notNull(ClassUtil.getResourceAsStream(browseFieldPath),
                RESOURCE_NOT_FOUND + browseFieldPath)) {
            objectDefinitions.withOperationFields(ImportableFields.from(inputStream).values());
        } catch (IOException e) {
            String errorMessage = String.format("Failed to read importable fields from %s", browseFieldPath);
            throw new ConnectorException(errorMessage, e);
        }
    }
}
