//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.operations.upload.multipart;

import com.boomi.util.IOUtil;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public abstract class AbstractPartWrapper implements Part, Closeable {

    final FilePart part;

    AbstractPartWrapper(FilePart filePart) {
        this.part = filePart;
    }

    @Override
    public void reset() throws IOException {
        part.reset();
    }

    @Override
    public String getDisposition() {
        return part.getDisposition();
    }

    @Override
    public void close(){
        IOUtil.closeQuietly(part);
    }
}
