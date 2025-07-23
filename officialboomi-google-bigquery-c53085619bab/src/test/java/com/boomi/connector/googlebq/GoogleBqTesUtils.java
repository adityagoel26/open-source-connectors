// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq;

import com.boomi.util.ClassUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.StreamUtil;
import com.boomi.util.TempOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class GoogleBqTesUtils {

    public static InputStream getResource(String resource) {
        return ClassUtil.getResourceAsStream(resource);
    }

    public static InputStream getCompressedResource(String resource) throws IOException {
        InputStream inputStream =  getResource(resource);
        GZIPOutputStream gzipOutputStream = null;
        try (TempOutputStream tempOutputStream = new TempOutputStream(inputStream.available())) {
            gzipOutputStream = new GZIPOutputStream(tempOutputStream);
            StreamUtil.copy(inputStream, gzipOutputStream);
            gzipOutputStream.finish();
            return tempOutputStream.toInputStream();
        }
        finally {
            IOUtil.closeQuietly(inputStream, gzipOutputStream);
        }
    }
}
