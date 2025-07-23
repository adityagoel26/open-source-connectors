//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.common;

import java.io.File;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.boomi.connector.sftp.actions.RetryableGetFileMetadataAction;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.util.StringUtil;

/**
 * The Class FileMetadata.
 *
 * @author Omesh Deoli
 * 
 *         
 */
public class FileMetadata {
	
	
	/** The file name. */
	private final String fileName;
	
	/** The parent. */
	private final String parent;
	
	

	
	/** The Constant FILE_PATH_SEPARATOR. */
	private static final String FILE_PATH_SEPARATOR = "/";

	/**
	 * Instantiates a new file metadata.
	 *
	 * @param path the path
	 * @param retryableFilemetadataction the retryable filemetadataction
	 */
	public FileMetadata(Path path, RetryableGetFileMetadataAction retryableFilemetadataction) {
		if (path == null) {
			throw new IllegalArgumentException(SFTPConstants.INVALID_PATH);
		}
		File file = path.toFile();
		this.fileName = file.getName();
		this.parent = file.getParent();
		retryableFilemetadataction.setFileName(this.fileName);
		retryableFilemetadataction.execute();
	}



	/**
	 * Gets the file name.
	 *
	 * @return the file name
	 */
	public String getFileName() {
		return this.fileName;
	}

	/**
	 * Gets the parent.
	 *
	 * @return the parent
	 */
	public String getParent() {
		return this.parent;
	}


	/**
	 * Format date.
	 *
	 * @param date the date
	 * @return the string
	 */
	public static String formatDate(Date date) {
		return FileMetadata.iso8601Format().format(date);
	}

	/**
	 * Parses the date.
	 *
	 * @param date the date
	 * @return the date
	 * @throws ParseException the parse exception
	 */
	public static Date parseDate(String date) throws ParseException {
		return new FileDate(FileMetadata.iso8601Format().parse(date));
	}

	/**
	 * Parses the date.
	 *
	 * @param date the date
	 * @return the date
	 */
	public static Date parseDate(long date) {
		return new FileDate(date);
	}

	/**
	 * Iso 8601 format.
	 *
	 * @return the simple date format
	 */
	private static SimpleDateFormat iso8601Format() {
		SimpleDateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
		utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		return utcFormat;
	}

	/**
	 * Join paths.
	 *
	 * @param parentPath the parent path
	 * @param childRelativePath the child relative path
	 * @return the string
	 */
	public static String joinPaths(String parentPath, String childRelativePath) {
		if (StringUtil.isBlank(parentPath)) {
			return childRelativePath;
		}
		if (StringUtil.isBlank(childRelativePath)) {
			return parentPath;
		}
		String result = parentPath + (!parentPath.endsWith(FILE_PATH_SEPARATOR) ? FILE_PATH_SEPARATOR : "")
				+ childRelativePath;
		return result.replace('\\', '/');
	}

	/**
	 * The Class FileDate.
	 */
	private static class FileDate extends Date {
		
		/** The Constant serialVersionUID. */
		private static final long serialVersionUID = 4045331070L;

		/**
		 * Instantiates a new file date.
		 *
		 * @param date the date
		 */
		FileDate(long date) {
			super(date);
		}

		/**
		 * Instantiates a new file date.
		 *
		 * @param date the date
		 */
		FileDate(Date date) {
			this(date.getTime());
		}

		/**
		 * To string.
		 *
		 * @return the string
		 */
		@Override
		public String toString() {
			return FileMetadata.formatDate(this);
		}
	}

}
