//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.boomi.connector.api.ConnectionTester;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.connector.api.FieldSpecField;
import com.boomi.connector.util.BaseBrowser;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The Class SFTPBrowser.
 *
 * @author Omesh Deoli
 * 
 * 
 */

public class SFTPBrowser extends BaseBrowser implements ConnectionTester {

	/**
	 * Instantiates a new SFTP browser.
	 *
	 * @param conn the conn
	 */
	@SuppressWarnings("unchecked")
	protected SFTPBrowser(SFTPConnection conn) {
		super(conn);
	}

	/** The Constant QUERY_FIELDS. */
	private static final List<FieldSpecField> QUERY_FIELDS = SFTPBrowser.initQueryFields();

	/** The Constant LIST_FIELDS. */
	private static final List<FieldSpecField> LIST_FIELDS = SFTPBrowser.initListFields();

	/**
	 * Gets the object definitions.
	 *
	 * @param objectTypeId the object type id
	 * @param roles the roles
	 * @return the object definitions
	 */
	@Override
	public ObjectDefinitions getObjectDefinitions(String objectTypeId, Collection<ObjectDefinitionRole> roles) {
		ObjectDefinitions objectDefinitions = new ObjectDefinitions();
		for (ObjectDefinitionRole role : roles) {
			objectDefinitions.withDefinitions(this.buildObjectDefinition(role));
		}
		return objectDefinitions;

	}

	/**
	 * Builds the object definition.
	 *
	 * @param role the role
	 * @return the object definition
	 */
	private ObjectDefinition buildObjectDefinition(ObjectDefinitionRole role) {
		ObjectDefinition objectDefinition = new ObjectDefinition();
		switch (this.getContext().getOperationType()) {
		case QUERY:

			if (this.isListOperation()) {
				return objectDefinition.withInputType(ContentType.NONE)
						.withOutputType(role == ObjectDefinitionRole.OUTPUT ? ContentType.BINARY : ContentType.NONE)
						.withFieldSpecFields(LIST_FIELDS);
			} else {
				return objectDefinition.withInputType(ContentType.NONE)
						.withOutputType(role == ObjectDefinitionRole.OUTPUT ? ContentType.BINARY : ContentType.NONE)
						.withFieldSpecFields(QUERY_FIELDS);
			}
		case UPSERT:
		case CREATE:
			return this.prepareCreateObjDefs(role);
		case DELETE:
			return objectDefinition.withInputType(ContentType.JSON).withOutputType(ContentType.NONE);
		case LISTEN:
		case GET:
			return objectDefinition.withInputType(ContentType.NONE).withOutputType(ContentType.BINARY);
		default:
			throw new UnsupportedOperationException();
		}

	}

	/**
	 * Prepare create obj defs.
	 *
	 * @param role the role
	 * @return the object definition
	 */
	private ObjectDefinition prepareCreateObjDefs(ObjectDefinitionRole role) {
		ObjectDefinition objectDefinition = new ObjectDefinition()
				.withInputType(role == ObjectDefinitionRole.INPUT ? ContentType.BINARY : ContentType.NONE)
				.withOutputType(role == ObjectDefinitionRole.OUTPUT ? ContentType.JSON : ContentType.NONE);
		if (role == ObjectDefinitionRole.OUTPUT) {
			boolean includeAllMetadata = this.getContext().getOperationProperties()
					.getBooleanProperty(SFTPConstants.PROPERTY_INCLUDE_METADATA, Boolean.FALSE);
			String outSchemaPath = includeAllMetadata ? SFTPConstants.EXTENDED_FILE_META_SCHEMA_PATH
					: SFTPConstants.SIMPLE_FILE_META_SCHEMA_PATH;
			ObjectNode jsonCookie = JSONUtil.newObjectNode().put(SFTPConstants.PROPERTY_INCLUDE_METADATA,
					includeAllMetadata);
			objectDefinition.withCookie(jsonCookie.toString()).withJsonSchema(SFTPBrowser.getSchema(outSchemaPath))
					.withElementName("");
		}
		return objectDefinition;
	}

	/**
	 * Gets the schema.
	 *
	 * @param outSchemaPath the out schema path
	 * @return the schema
	 */
	private static String getSchema(String outSchemaPath) {
		try {
			return JSONUtil.loadSchemaFromResource(outSchemaPath).toString();
		} catch (IOException e) {
			throw new ConnectorException(MessageFormat.format(SFTPConstants.ERROR_SCHEMA_LOAD_FORMAT, outSchemaPath),
					(Throwable) e);
		}
	}

	/**
	 * Gets the object types.
	 *
	 * @return the object types
	 */
	@Override
	public ObjectTypes getObjectTypes() {
		ObjectTypes types = new ObjectTypes();
		List<ObjectType> list = types.getTypes();
		switch (this.getContext().getOperationType()) {

		case QUERY:
			list.add(SFTPCustomType.valueOf(this.getContext().getCustomOperationType()).getObject().makeObjectType());
			return types;
		case UPSERT:
		case GET:
		case LISTEN:
		case CREATE:
		case DELETE:
			ObjectType objectType = new ObjectType().withId(SFTPConstants.OBJECT_TYPE_FILE)
					.withLabel(SFTPConstants.OBJECT_TYPE_FILE);
			return types.withTypes(objectType);
		default:
			throw new UnsupportedOperationException();
		}

	}

	/**
	 * Inits the query fields.
	 *
	 * @return the list
	 */
	private static List<FieldSpecField> initQueryFields() {
		String[] props = new String[] { SFTPConstants.FILESIZE, SFTPConstants.MODIFIED_DATE };
		ArrayList<FieldSpecField> fields = new ArrayList<>();
		fields.add(new FieldSpecField().withName(SFTPConstants.PROPERTY_FILENAME).withFilterable(Boolean.valueOf(true))
				.withType(SFTPConstants.PATH_TYPE));
		for (String prop : props) {
			fields.add(new FieldSpecField().withName(prop).withFilterable(Boolean.valueOf(true))
					.withType(SFTPConstants.COMPARABLE_TYPE));
		}
		return fields;
	}

	/**
	 * Checks if is list operation.
	 *
	 * @return true, if is list operation
	 */
	private boolean isListOperation() {
		return SFTPCustomType.LIST.name().equals(this.getContext().getCustomOperationType());
	}

	/**
	 * Inits the list fields.
	 *
	 * @return the list
	 */
	private static List<FieldSpecField> initListFields() {
		List<FieldSpecField> fields = SFTPBrowser.initQueryFields();
		fields.add(new FieldSpecField().withName(SFTPConstants.IS_DIRECTORY).withFilterable(Boolean.valueOf(true))
				.withType(SFTPConstants.BOOLEAN_TYPE));
		return fields;
	}

	/**
	 * Gets the connection.
	 *
	 * @return the connection
	 */
	@Override
	public SFTPConnection getConnection() {
		return (SFTPConnection) super.getConnection();
	}

	/**
	 * Test connection.
	 */
	@Override
	public void testConnection() {
			getConnection().testConnection();
	}
}
