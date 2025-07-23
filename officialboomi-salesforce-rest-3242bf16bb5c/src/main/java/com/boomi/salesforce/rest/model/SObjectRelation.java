// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.model;

/**
 * Model represents Salesforce SObject name and relationship name of a relationship
 */
public class SObjectRelation {
    private final String _relationshipSObject;
    private final String _relationshipName;

    public SObjectRelation(String relationshipSObject, String relationshipName) {
        _relationshipSObject = relationshipSObject;
        _relationshipName = relationshipName;
    }

    public String getRelationSObjectName() {
        return _relationshipSObject;
    }

    public String getRelationshipName() {
        return _relationshipName;
    }
}
