//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.common;

/**
  * The Interface PathsHandler.
  *
  * @author Omesh Deoli
  * 
  * 
  */
public interface PathsHandler {
	
	/**
	 * Split into dir and file name.
	 *
	 * @param var1 the var 1
	 * @return the SFTP file metadata
	 */
	public SFTPFileMetadata splitIntoDirAndFileName(String var1);

	/**
	 * Resolve paths.
	 *
	 * @param var1 the var 1
	 * @param var2 the var 2
	 * @return the string
	 */
	public String resolvePaths(String var1, String var2);

	/**
	 * Split into dir and file name.
	 *
	 * @param var1 the var 1
	 * @param var2 the var 2
	 * @return the SFTP file metadata
	 */
	public SFTPFileMetadata splitIntoDirAndFileName(String var1, String var2);

	/**
	 * Join paths.
	 *
	 * @param var1 the var 1
	 * @param var2 the var 2
	 * @return the string
	 */
	public String joinPaths(String var1, String var2);

	/**
	 * Checks if is full path.
	 *
	 * @param var1 the var 1
	 * @return true, if is full path
	 */
	public boolean isFullPath(String var1);
}
