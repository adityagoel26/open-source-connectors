//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.operations.upload.multipart;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public interface Part {

    void reset() throws IOException;

    String getDisposition();

    String getContentType();

    void write(OutputStream outputStream) throws IOException;

}
