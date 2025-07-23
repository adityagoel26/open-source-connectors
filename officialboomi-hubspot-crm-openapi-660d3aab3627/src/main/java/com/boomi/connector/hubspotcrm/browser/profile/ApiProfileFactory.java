// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.hubspotcrm.browser.profile;

import com.boomi.connector.hubspotcrm.browser.HubspotcrmObjectMetadataRetriever;
import com.boomi.connector.hubspotcrm.browser.HubspotcrmOperationType;

import org.json.JSONObject;

/**
 * This class extends the ProfileFactory and is responsible for creating API profiles
 * for the Hubspot CRM connector.
 */
class ApiProfileFactory extends ProfileFactory {

    ApiProfileFactory(HubspotcrmOperationType operationType,
            HubspotcrmObjectMetadataRetriever metadataRetriever) {
        super(operationType, metadataRetriever);
    }

    /**
     * Adds fields to the given profile based on the operation type.
     *
     * @param objectTypeId           the ID of the object type for which the profile is being created
     * @param profile                the JSONObject representing the profile
     * @param path                   the path of the field being added
     * @param depth                  the depth of the field being added
     * @param lookupRelationshipName the current relationship name of the fields
     */
    @Override
    public void addFieldsToProfile(String objectTypeId, JSONObject profile, String path, int depth,
            String lookupRelationshipName) {
        for (ProfileField field : getProfileFields(objectTypeId)) {
            switch (_operationType) {
                case CREATE:
                case UPDATE:
                case QUERY:
                    addFieldToProfile(profile, field);
                    break;
                default:
                    throw new IllegalArgumentException("unexpected operation type: " + _operationType);
            }
        }
    }
}