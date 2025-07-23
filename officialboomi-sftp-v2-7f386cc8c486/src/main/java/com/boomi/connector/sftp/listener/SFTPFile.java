//Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.sftp.listener;

import java.io.IOException;

import com.github.drapostolos.rdp4j.spi.FileElement;
import com.jcraft.jsch.ChannelSftp.LsEntry;

/**
 * The Class SftpFile
 * 
 * @author sweta.b.das
 */
public class SFTPFile implements FileElement
{
    
    /** The file. */
    private final LsEntry file;

    /**
     * Instantiates a new sftp file.
     *
     * @param file the file
     */
    public SFTPFile(LsEntry file) {
        this.file = file;
    }

   /**
    * Last modified.
    *
    * @return the long
    * @throws IOException Signals that an I/O exception has occurred.
    */
   @Override
    public long lastModified() throws IOException 
    {
    	return file.getAttrs().getMTime();
    }

   /**
    * Checks if is directory.
    *
    * @return true, if is directory
    */
   @Override
    public boolean isDirectory() {
        return false;
    }

   /**
    * Gets the name.
    *
    * @return the name
    */
   @Override
    public String getName() {
        return file.getFilename();
    }

   /**
    * To string.
    *
    * @return the string
    */
   @Override
    public String toString() {
        return getName();
    }
}
