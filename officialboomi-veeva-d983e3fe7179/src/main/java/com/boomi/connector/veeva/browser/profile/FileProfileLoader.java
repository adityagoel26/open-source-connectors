// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva.browser.profile;

import com.boomi.connector.api.ConnectorException;
import com.boomi.util.ClassUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.StreamUtil;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Utility class to load static profiles.
 */
public class FileProfileLoader {

    private FileProfileLoader() {
        // Utility class
    }

    /**
     * Return the output JSON profile schema for Create Vault, Update Vault and Documents related actions in Execute
     * operation.
     *
     * @param isDocument indicates if the profile is for a documents related execution action
     * @return the output JSON profile
     */
    public static String getStaticOutputResponseProfileSchema(boolean isDocument) {
        if (isDocument) {
            return readResource("document_edit_response.json");
        }
        return readResource("object_edit_response.json");
    }

    static String getQueryRelationshipsResponse() throws ConnectorException {
        return readResource("relationship_query_response.json");
    }

    static String readResource(String resourcePath) throws ConnectorException {
        InputStream stream = null;
        try {
            stream = ClassUtil.getResourceAsStream(resourcePath);

            if (stream == null) {
                throw new ConnectorException("The resource '" + resourcePath + "' cannot be found.");
            }

            return StreamUtil.toString(stream, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new ConnectorException("Error loading resource '" + resourcePath + "'", e);
        } finally {
            IOUtil.closeQuietly(stream);
        }
    }
}
