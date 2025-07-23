// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.controller.metadata;

import com.boomi.connector.api.OperationType;
import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.data.SobjectListExtractor;
import com.boomi.salesforce.rest.data.SobjectModelExtractor;
import com.boomi.salesforce.rest.model.SObjectField;
import com.boomi.salesforce.rest.model.SObjectModel;
import com.boomi.salesforce.rest.model.SObjectRelation;
import com.boomi.util.IOUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Responsible for parsing JSON metadata response and closing the consumed streams
 */
public class MetadataParser {

    private static final String RELATIONSHIP_NAME_KEY = "relationshipName";
    private static final String REFERENCE_TO_KEY = "referenceTo";
    private static final String CHILD_S_OBJECT_KEY = "childSObject";
    private static final String NAME_KEY = "name";
    private static final String TYPE_KEY = "type";
    private static final String PRECISION_KEY = "precision";
    private static final String SCALE_KEY = "scale";
    private static final String ID_LOOKUP_KEY = "idLookup";

    private MetadataParser() {
    }

    /**
     * Gets the list of SObject names matching given attribute (for example "queryable") of InputStream contains 'list
     * SObjects' JSON response.
     *
     * @param response  InputStream contains 'list SObjects' response
     * @param attribute required SObject property name (for example 'queryable')
     * @return List<String> of SObjects names
     */
    public static List<String> parseSObjectsOfAttribute(InputStream response, String attribute) throws IOException {
        List<String> ret = new ArrayList<>();
        SobjectListExtractor splitter = null;
        try {
            splitter = new SobjectListExtractor(response, Arrays.asList(attribute, NAME_KEY));
            while (splitter.parseSObject()) {
                // if SObject attribute is true returns its name
                if (Boolean.parseBoolean(splitter.getValue(attribute))) {
                    ret.add(splitter.getValue(NAME_KEY));
                }
            }
        } finally {
            IOUtil.closeQuietly(splitter);
        }
        return ret;
    }

    /**
     * Returns SObjectModel for a specific operation from a given 'describe SObject' Json response.
     *
     * @param response  InputStream 'describe SObject' response
     * @param operation target BOOMI operation
     * @return SObjectModel
     * @throws IOException if failed to read
     */
    public static SObjectModel parseFieldsForOperation(InputStream response, String operation) throws IOException {
        SObjectModel ret = null;
        if (OperationType.QUERY.name().equals(operation)) {
            ret = parseQueryFields(response);
        } else if (OperationType.DELETE.name().equals(operation)) {
            ret = parseDeleteFields();
        } else if (OperationType.CREATE.name().equals(operation)) {
            ret = parseCreateFields(response);
        } else if (OperationType.UPDATE.name().equals(operation)) {
            ret = parseUpdateFields(response);
        } else if (OperationType.UPSERT.name().equals(operation)) {
            ret = parseUpdateFields(response);
        }
        return ret;
    }

    /**
     * @param splitter
     * @return
     */
    private static SObjectModel parseRelationships(SobjectModelExtractor splitter, String fieldTag) throws IOException {
        List<SObjectField> fields = new ArrayList<>();
        List<SObjectRelation> children = new ArrayList<>();
        List<SObjectRelation> parents = new ArrayList<>();
        while (splitter.parseSObject()) {
            if (splitter.isFieldModel()) {
                if (fieldTag == null || Boolean.parseBoolean(splitter.getValue(fieldTag))) {
                    fields.add(buildFieldModel(splitter));
                }
                String relationshipName = splitter.getValue(RELATIONSHIP_NAME_KEY);
                String referenceTo = splitter.getValue(REFERENCE_TO_KEY);
                if (relationshipName != null && referenceTo != null) {
                    parents.add(new SObjectRelation(referenceTo, relationshipName));
                }
            } else if (splitter.isChildModel()) {
                String relationshipName = splitter.getValue(RELATIONSHIP_NAME_KEY);
                String childSObject = splitter.getValue(CHILD_S_OBJECT_KEY);
                if (relationshipName != null && childSObject != null) {
                    children.add(new SObjectRelation(childSObject, relationshipName));
                }
            }
        }
        return new SObjectModel(fields, children, parents);
    }

    /**
     * Returns SObjectModel for a 'QUERY' operation from a given 'describe SObject' Json response.
     *
     * @param response contains the split 'describe SObject' response
     * @return SObjectModel
     * @throws IOException if failed to read
     */
    private static SObjectModel parseQueryFields(InputStream response) throws IOException {
        SobjectModelExtractor splitter = null;
        try {
            splitter = new SobjectModelExtractor(response,
                    Arrays.asList(RELATIONSHIP_NAME_KEY, REFERENCE_TO_KEY, NAME_KEY, TYPE_KEY, SCALE_KEY, PRECISION_KEY,
                            CHILD_S_OBJECT_KEY));

            return parseRelationships(splitter, null);
        } finally {
            IOUtil.closeQuietly(splitter, response);
        }
    }

    /**
     * Returns SObjectModel for a 'DELETE' operation from a given 'describe SObject' Json response.
     * <br>
     * Gets the fields names and its types (Only the "Id" Field) for Delete Operations.
     *
     * @return SObjectModel
     * @throws IOException if failed to read
     */
    private static SObjectModel parseDeleteFields() {
        List<SObjectField> fields = new ArrayList<>();
        fields.add(new SObjectField("Id", "id"));
        return new SObjectModel(fields);
    }

    /**
     * Returns SObjectModel for a 'CREATE' operation from a given 'describe SObject' Json response
     *
     * @param response contains the split 'describe SObject' response
     * @return SObjectModel
     * @throws IOException if failed to read
     */
    private static SObjectModel parseCreateFields(InputStream response) throws IOException {
        SobjectModelExtractor splitter = null;
        try {
            splitter = new SobjectModelExtractor(response,
                    Arrays.asList(Constants.CREATEABLE, RELATIONSHIP_NAME_KEY, REFERENCE_TO_KEY, NAME_KEY, TYPE_KEY,
                            SCALE_KEY, PRECISION_KEY, CHILD_S_OBJECT_KEY));

            return parseRelationships(splitter, Constants.CREATEABLE);
        } finally {
            IOUtil.closeQuietly(splitter, response);
        }
    }

    /**
     * Returns SObjectModel for a 'UPDATE' operation from a given 'describe SObject' Json response.
     * <br>
     * Extra fields are the unique identifiers like 'Id' and 'Lookup' fields.
     *
     * @param response contains the split 'describe SObject' response
     * @return SObjectModel
     * @throws IOException if failed to read
     */
    private static SObjectModel parseUpdateFields(InputStream response) throws IOException {
        List<SObjectField> fields = new ArrayList<>();
        SobjectModelExtractor splitter = null;
        try {
            splitter = new SobjectModelExtractor(response,
                    Arrays.asList(Constants.UPDATEABLE, RELATIONSHIP_NAME_KEY, ID_LOOKUP_KEY, NAME_KEY, TYPE_KEY,
                            SCALE_KEY, PRECISION_KEY, CHILD_S_OBJECT_KEY));

            while (splitter.parseSObject()) {
                if (isUpdateable(splitter) || isLookupKey(splitter)) {
                    fields.add(buildFieldModel(splitter));
                }
            }
            return new SObjectModel(fields);
        } finally {
            IOUtil.closeQuietly(splitter, response);
        }
    }

    private static boolean isUpdateable(SobjectModelExtractor extractor) {
        return extractor.isFieldModel() && Boolean.parseBoolean(extractor.getValue(Constants.UPDATEABLE));
    }

    private static boolean isLookupKey(SobjectModelExtractor extractor) {
        return extractor.isFieldModel() && Boolean.parseBoolean(extractor.getValue(ID_LOOKUP_KEY));
    }

    /**
     * Builds a Field Model from a given parsed JSON Node metadata for a single field
     *
     * @param fieldSplitter
     * @return SObjectField
     */
    private static SObjectField buildFieldModel(SobjectModelExtractor fieldSplitter) {
        String name = fieldSplitter.getValue(NAME_KEY);
        String type = fieldSplitter.getValue(TYPE_KEY);
        int precision = Integer.parseInt(fieldSplitter.getValue(PRECISION_KEY));
        int scale = Integer.parseInt(fieldSplitter.getValue(SCALE_KEY));
        return new SObjectField(name, type, precision, scale);
    }
}
