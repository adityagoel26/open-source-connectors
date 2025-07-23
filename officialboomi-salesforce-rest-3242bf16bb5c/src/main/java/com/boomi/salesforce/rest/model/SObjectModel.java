// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.model;

import java.util.ArrayList;
import java.util.List;

/**
 * SObject model contains data from one describe SObject call
 */
public class SObjectModel {
    private final List<SObjectField> _fields;
    private final List<SObjectRelation> _children;
    private final List<SObjectRelation> _parents;

    public SObjectModel(List<SObjectField> fields) {
        this(fields, new ArrayList<>(), new ArrayList<>());
    }

    public SObjectModel(List<SObjectField> fields, List<SObjectRelation> children,
                        List<SObjectRelation> parents) {
        _fields = fields;
        _children = children;
        _parents = parents;
    }

    /**
     * @return list of SObjectField of the SObject
     */
    public List<SObjectField> getFields() {
        return _fields;
    }

    /**
     * @return list of ChildSObject describes the children relationship
     */
    public List<SObjectRelation> getChildren() {
        return _children;
    }

    /**
     * @return list of SObjectRelation describes the parents relationship
     */
    public List<SObjectRelation> getParents() {
        return _parents;
    }

}
