//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp;


import com.jcraft.jsch.DHGEX;

/**
 * The Class DHGEX1024.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class DHGEX1024
extends DHGEX {
    
    /** The Constant MAX_KEYSIZE. */
    private static final int MAX_KEYSIZE = 1024;

    /**
     * Check 2048.
     *
     * @param c the c
     * @param max the max
     * @return the int
     * @throws Exception the exception
     */
    @Override
    protected int check2048(@SuppressWarnings("rawtypes") Class c, int max) throws Exception {
        return max > MAX_KEYSIZE ? MAX_KEYSIZE : max;
    }
}

