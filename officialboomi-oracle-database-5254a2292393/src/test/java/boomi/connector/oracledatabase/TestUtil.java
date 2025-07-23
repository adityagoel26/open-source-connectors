// Copyright (c) 2023 Boomi, Inc.
package boomi.connector.oracledatabase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Logger;

import static org.mockito.Mockito.mock;

public final class TestUtil {

    private static Logger logger = mock(Logger.class);

    private TestUtil() {

    }

    public static String readJsonFromFile(String inputFile) {
        String text = null;
        try {
            text = new String(Files.readAllBytes(Paths.get(inputFile)));
        } catch (IOException e) {
            logger.info("Error occured in Test class.");
        }
        return text;
    }
}
