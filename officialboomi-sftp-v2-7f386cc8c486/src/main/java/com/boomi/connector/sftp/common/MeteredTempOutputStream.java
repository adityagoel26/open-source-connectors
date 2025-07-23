//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.common;

import com.boomi.util.TempOutputStream;
import java.io.IOException;

/**
 * The Class MeteredTempOutputStream.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class MeteredTempOutputStream
extends TempOutputStream {
    
    /** The count. */
    private long count;

    /**
     * Gets the count.
     *
     * @return the count
     */
    public long getCount() {
        return this.count;
    }

    /**
     * Write.
     *
     * @param b the b
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Override
    public void write(int b) throws IOException {
        super.write(b);
        ++this.count;
    }

    /**
     * Write.
     *
     * @param b the b
     * @param off the off
     * @param len the len
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        super.write(b, off, len);
        this.count += (long)len;
    }
}

