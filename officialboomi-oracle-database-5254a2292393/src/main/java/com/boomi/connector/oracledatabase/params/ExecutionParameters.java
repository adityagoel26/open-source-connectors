// Copyright (c) 2022 Boomi, Inc.

package com.boomi.connector.oracledatabase.params;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.Payload;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @author mukulrana.
 */
public class ExecutionParameters {
    private Connection con;
    private List<ObjectData> trackedData;
    private ObjectData objdata;
    private Payload payload;
    private OperationResponse response;
    private Map<String, String> dataTypes;
    List<String> primaryKey;
    Map<String, List<String>> uniqueKeys;
    private ExecutionParameters(){
        // Hide implicit constructor
    }

    public ExecutionParameters (Connection con, OperationResponse response, List<ObjectData> trackedData, ObjectData objdata, Payload payload){
        this.con = con;
        this.response = response;
        this.trackedData = trackedData;
        this.objdata = objdata;
        this.payload =payload;
    }

    public ExecutionParameters (Connection con, OperationResponse response, ObjectData objdata, Payload payload){
        this.con = con;
        this.response = response;
        this.objdata = objdata;
        this.payload =payload;
    }

    public ExecutionParameters (Connection con, OperationResponse response, ObjectData objdata){
        this.con = con;
        this.response = response;
        this.objdata = objdata;
    }

    public ExecutionParameters (Connection con, OperationResponse response, List<ObjectData> trackedData){
        this.con = con;
        this.response = response;
        this.trackedData = trackedData;
    }

    public ExecutionParameters(Connection con, Map<String, String> dataTypes) {
        this.con = con;
        this.dataTypes = dataTypes;
    }

    public ExecutionParameters(Connection con, OperationResponse response, ObjectData objdata, Map<String, String> dataTypes) {
        this.con = con;
        this.response = response;
        this.objdata = objdata;
        this.dataTypes = dataTypes;
    }

    public ExecutionParameters(Connection con, OperationResponse response, ObjectData objdata, Map<String, String> dataTypes, Payload payload) {
        this.con = con;
        this.response = response;
        this.objdata = objdata;
        this.dataTypes = dataTypes;
        this.payload = payload;
    }

    public ExecutionParameters(Map<String, String> dataTypes, OperationResponse response, List<ObjectData> trackedData, Connection con, List<String> primaryKey, Map<String, List<String>> uniqueKeys) {
        this.con = con;
        this.response = response;
        this.trackedData = trackedData;
        this.dataTypes = dataTypes;
        this.primaryKey = primaryKey;
        this.uniqueKeys = uniqueKeys;
    }

    public List<ObjectData> getTrackedData() {
        return trackedData;
    }

    public ObjectData getObjdata() {
        return objdata;
    }

    public Payload getPayload() {
        return payload;
    }

    public Connection getCon() {
        return con;
    }

    public OperationResponse getResponse() {
        return response;
    }

    public Map<String, String> getDataTypes() {
        return dataTypes;
    }

    public List<String> getPrimaryKey() {
        return primaryKey;
    }

    public Map<String, List<String>> getUniqueKeys() {
        return uniqueKeys;
    }
}
