//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.util.json.JSONUtil;
import java.io.IOException;

/**
 * The Enum SFTPObject.
 *
 * @author Omesh Deoli
 */
public enum SFTPObject {

	/** The file. */
	FILE("File"),
	/** The file create upsert. */
	FILE_CREATE_UPSERT("File", null, "file-schema.json"),

	/** The directory. */
	DIRECTORY("Directory", null, "file-schema.json");

	/** The label. */
	private final String label;

	/** The out schema. */
	private final String outSchema;

	/** The in schema. */
	private final String inSchema;

	/**
	 * Instantiates a new SFTP object.
	 *
	 * @param label the label
	 */
	private SFTPObject(String label) {
		this(label, null, null);
	}

	/**
	 * Instantiates a new SFTP object.
	 *
	 * @param label     the label
	 * @param inSchema  the in schema
	 * @param outSchema the out schema
	 */
	private SFTPObject(String label, String inSchema, String outSchema) {
		this.label = label;
		this.outSchema = outSchema;
		this.inSchema = inSchema;
	}

	/**
	 * Gets the label.
	 *
	 * @return the label
	 */
	public String getLabel() {
		return this.label;
	}

	/**
	 * Define.
	 *
	 * @param role the role
	 * @return the object definition
	 */
	public ObjectDefinition define(ObjectDefinitionRole role) {
		ObjectDefinition def;
		String schema = this.getSchema(role);
		if (schema == null) {
			def = new ObjectDefinition();
			def.setElementName("");
			if (role == ObjectDefinitionRole.INPUT) {
				def.withInputType(ContentType.BINARY).withOutputType(ContentType.NONE);
			} else {
				def.withInputType(ContentType.NONE).withOutputType(ContentType.BINARY);
			}
		} else {
			try {
				def = JSONUtil.newJsonDefinitionFromResource(role, schema);
			} catch (IOException e) {
				throw new ConnectorException(SFTPConstants.ERROR_CREATING_JSON_DEFINITION, (Throwable) e);
			}
		}
		return def;
	}

	/**
	 * Make object type.
	 *
	 * @return the object type
	 */
	public ObjectType makeObjectType() {
		return new ObjectType().withId(this.name()).withLabel(this.getLabel());
	}

	/**
	 * Gets the schema.
	 *
	 * @param role the role
	 * @return the schema
	 */
	private String getSchema(ObjectDefinitionRole role) {
		switch (role) {
		case INPUT:
			return this.inSchema;
		case OUTPUT:
			return this.outSchema;
		default:
			break;
		}
		throw new IllegalArgumentException(SFTPConstants.UNKNOWN_DEFINITION_ROLE + (Object) role);
	}

}
