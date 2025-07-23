//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp;

import com.boomi.connector.api.OperationType;

/**
 * The Enum SFTPCustomType.
 *
 * @author Omesh Deoli
 */
public enum SFTPCustomType {
	
	/** The list. */
	LIST(OperationType.QUERY, SFTPObject.DIRECTORY), 
 /** The query. */
 QUERY(OperationType.QUERY, SFTPObject.FILE);

	/** The super type. */
	private final OperationType superType;
	
	/** The object. */
	private final SFTPObject object;

	/**
	 * Instantiates a new SFTP custom type.
	 *
	 * @param superType the super type
	 * @param object the object
	 */
	private SFTPCustomType(OperationType superType, SFTPObject object) {
		this.superType = superType;
		this.object = object;
	}

	/**
	 * Gets the super type.
	 *
	 * @return the super type
	 */
	public OperationType getSuperType() {
		return this.superType;
	}

	/**
	 * Gets the object.
	 *
	 * @return the object
	 */
	public SFTPObject getObject() {
		return this.object;
	}
}
