//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.operations.upload;

import com.boomi.util.LengthLimitedInputStream;
import com.boomi.util.StreamUtil;
import java.io.IOException;
import java.io.InputStream;

/**
 * This is a custom implementation of {@link LengthLimitedInputStream} to handle a reset to the provided
 * initial offset.
 *
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class ResettableLengthLimitedInputStream extends LengthLimitedInputStream {

    private final long initialOffset;

    /**
     * Creates a {@link ResettableLengthLimitedInputStream}
     *
     * @param content
     *         an {@link InputStream} instance with the content.
     * @param limit
     *         content size
     * @param initialOffset
     *         initial marker
     * @throws IOException
     *         the superclass throws an IOException if the content cannot be skipped to the initialOffset mark.
     */
    ResettableLengthLimitedInputStream(InputStream content, long limit, long initialOffset) throws IOException {
        super(content, limit, false, initialOffset);
        this.initialOffset = initialOffset;
    }

    /**
     * Resets the content and skip it up to the initial offset value provided in the constructor.
     *
     * @throws IOException
     *         if the wrapped InputStream cannot be reset.
     */
    @Override
    public synchronized void reset() throws IOException {
        super.reset();
        StreamUtil.skipFully(in, initialOffset);
    }
}
