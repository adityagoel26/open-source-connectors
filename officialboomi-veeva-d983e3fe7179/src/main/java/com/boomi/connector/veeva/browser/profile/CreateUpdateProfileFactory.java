// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva.browser.profile;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.veeva.browser.VeevaObjectMetadataRetriever;
import com.boomi.connector.veeva.browser.VeevaOperationType;

import org.json.JSONObject;

/**
 * Implementation of {@link ProfileFactory} for Create and Update operations profiles
 */
class CreateUpdateProfileFactory extends ProfileFactory {

    private static final String REQUIRED_FIELDS_ONLY_BROWSE_PROPERTY = "REQUIRED_FIELDS_ONLY";

    private final boolean _includeAllFields;

    CreateUpdateProfileFactory(VeevaOperationType operationType, BrowseContext context,
            VeevaObjectMetadataRetriever metadataRetriever) {
        super(operationType, metadataRetriever);
        _includeAllFields = !context.getOperationProperties().getBooleanProperty(REQUIRED_FIELDS_ONLY_BROWSE_PROPERTY,
                false);
    }

    @Override
    void addFieldsToProfile(String objectTypeId, JSONObject profile, String path, int depth,
            String lookupRelationshipName) {
        for (ProfileField field : getProfileFields(objectTypeId)) {
            String fieldName = field.getName();
            boolean isRequired = field.isRequired() || _includeAllFields;
            switch (_operationType) {
                case CREATE:
                    if (isRequired && (field.isEditable() || field.isSetOnCreateOnly())) {
                        addFieldToProfile(profile, field);
                    }
                    break;
                case UPDATE:
                    if (ID.equals(fieldName) || (isRequired && field.isEditable())) {
                        addFieldToProfile(profile, field);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("unexpected operation type: " + _operationType);
            }
        }
    }
}
