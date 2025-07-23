/*
 * Copyright (c) 2021 Boomi, Inc.
 */

package com.boomi.connector.odataclient;

public class ODataConstants {
    public static final String VALUE="VALUE_";
    public static final String HTTP_JSON_SCHEMA_ORG_DRAFT_04_SCHEMA = "http://json-schema.org/draft-04/schema#";

    public static final String CHILD_KEYS_ELEMENT = "___Link to Existing Object";
    public static final String CHILD_KEYS_ELEMENT_DESCRIPTION = "Specify key field values to link to an existing instance of a related child object.";
    public static final String SCHEMA = "$schema";
    public static final String OBJECT = "object";

    public static final String PATCH = "PATCH";
    public static final String FALSE = "false";
    public static final String POST = "POST";
    public static final String DELETE = "DELETE";
    public static final String TYPE = "type";
    public static final String ITEMS = "items";
    public static final String GET="GET";
    public static final String ARRAY="array";
    public static final String TITLE="title";
    public static final String DESCRIPTION="description";
    public static final String RESULTS="results";
    public static final String METADATA="$metadata";
    public static final String APPLICATIONXML="application/xml";

    public static final String PROPERTIES = "properties";
    public static final String STRING = "String";


    public static final String ACCEPT="Accept";
    public static final String AUTHORIZATION="Authorization";
    public static final String TOKEN="x-csrf-token";
    public static final String FETCH="Fetch";
    public static final String COOKIE="Cookie";
    public static final String MATCH="If-Match";
    public static final String APPLICATIONJSON = "application/json";
    public static final String Etag ="etag";
    public static final String SETCOOKIE ="set-cookie";

    public static final String FETCH_HEADERS="FETCH_HEADERS";
    public static final String PUT="PUT";
    public static final String EXECUTE="EXECUTE";

    public static final String ALLOWSELECT="ALLOWSELECT";
    public static final String EXTRAURIPARAMS="extraURIParams";
    public static final String D_RESULTS = "/d/results/*";

    public static final String MAXLENGTH="maxLength";
    public static final String FORMAT="format";

    public static final String UTC="UTC";

    public static final String ISNAVIGATIONPROPERTY="isNavigationProperty";
    public static final String SCALE="scale";
    public static final String ISKEY="isKey";
    public static final String ISARRAY="isArray";
    public static final String ENTITYSETNAME="entitySetName";
    public static final String DEEPCREATEMODE="deepCreateMode";
    public static final String HTTPMETHOD="httpMethod";
    public static final String string="string";
    public static final String ETAG="ETAG";
    public static final String XCSRF="X_CSRF_TOKEN";
    public static final String TOP="$top";
    public static final String SAP_SESSIONID_COOKIE = "SAP_SESSIONID_COOKIE";
    

    private ODataConstants() {
    }

}
