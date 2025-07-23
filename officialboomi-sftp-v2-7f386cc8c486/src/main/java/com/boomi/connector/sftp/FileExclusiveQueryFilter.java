//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp;

import com.boomi.connector.api.FilterData;
import com.jcraft.jsch.ChannelSftp.LsEntry;

import java.io.File;

/**
 * The Class FileExclusiveQueryFilter.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class FileExclusiveQueryFilter extends FileQueryFilter {
	
	/**
	 * Instantiates a new file exclusive query filter.
	 *
	 * @param directory the directory
	 * @param input the input
	 * @param dirPath the dir path
	 */
	public FileExclusiveQueryFilter(File directory, FilterData input, String dirPath) {
		super(directory, input, dirPath);
	}

	/**
	 * Accept.
	 *
	 * @param entry the entry
	 * @return true, if successful
	 */
	@Override
	public boolean accept(LsEntry entry) {
		return (!entry.getAttrs().isLink()) && (!entry.getAttrs().isDir()) && super.accept(entry);
	}

}
