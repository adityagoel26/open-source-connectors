//Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.sftp.listener;

import com.boomi.connector.sftp.SFTPConnection;
import com.github.drapostolos.rdp4j.spi.FileElement;
import com.github.drapostolos.rdp4j.spi.PolledDirectory;
import com.jcraft.jsch.ChannelSftp.LsEntry;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The Class SFTPDirectory.
 * @author sweta.b.das
 */
public class SFTPDirectory implements PolledDirectory {

	/** The connection. */
	private SFTPConnection connection;
	
	/** The remote directory. */
	private String remoteDirectory;

	/**
	 * Instantiates a new SFTP directory.
	 *
	 * @param connection the connection
	 * @param remoteDirectory the remote directory
	 */
	public SFTPDirectory(SFTPConnection connection, String remoteDirectory) {
		this.connection = connection;
		this.remoteDirectory = remoteDirectory;
	}

	/**
	 * List files.
	 *
	 * @return the sets the
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public Set<FileElement> listFiles() {
		Set<FileElement> result = new LinkedHashSet<>();
		List<LsEntry> filesList = connection.listDirectoryContent(remoteDirectory);
		for (LsEntry file : filesList) {
			result.add(new SFTPFile(file));

		}
		return result;
	}

}