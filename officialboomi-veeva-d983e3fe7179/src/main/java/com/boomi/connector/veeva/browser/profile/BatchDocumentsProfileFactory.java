// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva.browser.profile;

import com.boomi.connector.veeva.browser.VeevaObjectMetadataRetriever;
import com.boomi.connector.veeva.browser.VeevaOperationType;
import com.boomi.util.CollectionUtil;

import org.json.JSONObject;

import java.util.Set;

/**
 * Implementation of {@link ProfileFactory} for Batch Documents operations profiles
 */
class BatchDocumentsProfileFactory extends ProfileFactory {

    private static final String TYPE_V = "type__v";
    private static final String SUBTYPE_V = "subtype__v";
    private static final String CLASSIFICATION_V = "classification__v";
    private static final String STRING = "string";
    private static final String KEY_FILE = "file";
    private static final String KEY_FROM_TEMPLATE = "fromTemplate";
    private static final String KEY_TYPE = "type";
    private static final Set<String> REQUIRED_DOCUMENTS_FIELDS = CollectionUtil.asImmutableSet(CLASSIFICATION_V,
            SUBTYPE_V, TYPE_V);

    BatchDocumentsProfileFactory(VeevaOperationType operationType, VeevaObjectMetadataRetriever metadataRetriever) {
        super(operationType, metadataRetriever);
    }

    @Override
    void addFieldsToProfile(String objectTypeId, JSONObject profile, String path, int depth,
            String lookupRelationshipName) {
        if (VeevaOperationType.CREATE == _operationType) {
            addStringField(profile, KEY_FILE);
            addStringField(profile, KEY_FROM_TEMPLATE);
        }

        for (ProfileField field : getProfileFields(objectTypeId)) {
            String fieldName = field.getName();
            switch (_operationType) {
                case CREATE:
                    boolean isEditable = field.isEditable() || field.isSetOnCreateOnly();
                    if (REQUIRED_DOCUMENTS_FIELDS.contains(fieldName) || isEditable) {
                        addFieldToProfile(profile, field);
                    }
                    break;
                case UPDATE:
                    if (REQUIRED_DOCUMENTS_FIELDS.contains(fieldName) || ID.equals(fieldName) || field.isEditable()) {
                        addFieldToProfile(profile, field);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("unexpected operation type: " + _operationType);
            }
        }
    }

    private static void addStringField(JSONObject profile, String keyFile) {
        profile.put(keyFile, new JSONObject().put(KEY_TYPE, STRING));
    }
}
