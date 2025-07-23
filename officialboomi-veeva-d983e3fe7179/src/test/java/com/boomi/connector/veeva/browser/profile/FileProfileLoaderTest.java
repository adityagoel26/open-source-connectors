// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva.browser.profile;

import com.boomi.connector.api.ConnectorException;
import com.boomi.util.ClassUtil;
import com.boomi.util.StreamUtil;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

public class FileProfileLoaderTest {

    @Test
    void getDocumentEditResponseSchemaTest() {
        String documentEditResponseSchema = FileProfileLoader.getStaticOutputResponseProfileSchema(true);
        assertTrue(documentEditResponseSchema.contains("external_id__v"));
    }

    @Test
    void getObjectEditResponseSchemaTest() {
        String objectEditResponseSchema = FileProfileLoader.getStaticOutputResponseProfileSchema(false);
        String expectedContent = "\"data\": {\n              \"type\": \"object\",";
        assertTrue(objectEditResponseSchema.contains(expectedContent));
    }

    @Test
    void readResourceExceptionTest() {
        try (MockedStatic<ClassUtil> classUtilMock = mockStatic(ClassUtil.class)) {
            classUtilMock.when(() -> ClassUtil.getResourceAsStream("document_edit_response.json")).thenReturn(null);
            Throwable t = assertThrows(ConnectorException.class, () -> FileProfileLoader.getStaticOutputResponseProfileSchema(true));
            assertEquals("The resource 'document_edit_response.json' cannot be found.", t.getMessage());
        }
    }

    @Test
    void readResourceIOExceptionTest() {
        try (MockedStatic<StreamUtil> streamUtilMock = mockStatic(StreamUtil.class)) {
            streamUtilMock.when(() -> StreamUtil.toString(ArgumentMatchers.any(InputStream.class),
                    ArgumentMatchers.any(Charset.class))).thenThrow(IOException.class);
            Throwable t = assertThrows(ConnectorException.class, () -> FileProfileLoader.getStaticOutputResponseProfileSchema(true));
            assertEquals("Error loading resource 'document_edit_response.json': java.io.IOException", t.getMessage());
        }
    }
}
