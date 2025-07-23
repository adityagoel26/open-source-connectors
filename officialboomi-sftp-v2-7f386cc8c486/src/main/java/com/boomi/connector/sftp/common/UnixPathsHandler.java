//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.common;

import com.boomi.util.StringUtil;

/**
 * The Class UnixPathsHandler.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class UnixPathsHandler implements PathsHandler {
	
	/** The Constant FILE_PATH_SEPARATOR. */
	private static final String FILE_PATH_SEPARATOR = "/";

	/**
	 * Split into dir and file name.
	 *
	 * @param path the path
	 * @return the SFTP file metadata
	 */
	@Override
	public SFTPFileMetadata splitIntoDirAndFileName(String path) {
		String fileName;
		String dirPath;
		if (path == null) {
			return null;
		}
		path = removeExtraTrailingSeparators(path);
		int lastSlashPosition = path.lastIndexOf(FILE_PATH_SEPARATOR);
		if (0 < lastSlashPosition && lastSlashPosition < path.length()) {
			dirPath = path.substring(0, lastSlashPosition);
			fileName = path.substring(lastSlashPosition + 1);
		} else if (lastSlashPosition == 0) {
			dirPath = FILE_PATH_SEPARATOR;
			fileName = path.substring(1);
		} else {
			dirPath = "";
			fileName = path;
		}
		return new SimpleSFTPFileMetadata(dirPath, fileName);
	}

	/**
	 * Resolve paths.
	 *
	 * @param parentPath the parent path
	 * @param childPath the child path
	 * @return the string
	 */
	@Override
	public String resolvePaths(String parentPath, String childPath) {
		if (StringUtil.isBlank(childPath)) {
			return parentPath;
		}
		if (this.isFullPath(childPath)) {
			return childPath;
		}
		return this.joinPaths(parentPath, childPath);
	}

	/**
	 * Split into dir and file name.
	 *
	 * @param parentPath the parent path
	 * @param childPath the child path
	 * @return the SFTP file metadata
	 */
	@Override
	public SFTPFileMetadata splitIntoDirAndFileName(String parentPath, String childPath) {
		return this.splitIntoDirAndFileName(this.resolvePaths(parentPath, childPath));
	}

	/**
	 * Join paths.
	 *
	 * @param parentPath the parent path
	 * @param childRelativePath the child relative path
	 * @return the string
	 */
	@Override
	public String joinPaths(String parentPath, String childRelativePath) {
		if (StringUtil.isBlank(parentPath)) {
			return childRelativePath;
		}
		if (StringUtil.isBlank(childRelativePath)) {
			return parentPath;
		}
		return parentPath + (!parentPath.endsWith(FILE_PATH_SEPARATOR) ? FILE_PATH_SEPARATOR : "") + childRelativePath;
	}

	/**
	 * Checks if is full path.
	 *
	 * @param path the path
	 * @return true, if is full path
	 */
	@Override
	public boolean isFullPath(String path) {
		return StringUtil.startsWith( path, FILE_PATH_SEPARATOR);
	}

	/**
	 * Removes the extra trailing separators.
	 *
	 * @param path the path
	 * @return the string
	 */
	private static String removeExtraTrailingSeparators(String path) {
		while (StringUtil.endsWith(path, FILE_PATH_SEPARATOR) && path.length() > 1) {
			path = path.substring(0, path.length() - 1);
		}
		return path;
	}
}
