// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.builder.schema;

import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.salesforce.rest.controller.metadata.SObjectController;
import com.boomi.salesforce.rest.model.SObjectField;
import com.boomi.util.DOMUtil;
import com.boomi.util.StringUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Responsible for dynamically building XML Schema for importing salesforce profiles, handles the parent-child
 * relationship hierarchy too
 */
public abstract class XMLSchemaBuilder {
    protected static final String XSD_TAG_SEQUENCE = "xs:sequence";
    protected static final String XSD_TAG_ELEMENT = "xs:element";
    protected static final String XSD_TAG_COMPLEXTYPE = "xs:complexType";
    protected static final String ATTR_NAME = "name";
    protected static final String ATTR_TYPE = "type";
    protected static final String ATTR_MINOCCURS = "minOccurs";
    protected static final String ATTR_MAXOCCURS = "maxOccurs";
    protected static final String TAG_SALESFORCE_DONE = "done";
    protected static final String TAG_SALESFORCE_SIZE = "totalSize";
    protected static final String TAG_ATTRIBUTE = "xs:attribute";
    protected static final String KEY_IGNORE_SORT = "is";
    private static final String XSD_TAG_SCHEMA = "xs:schema";
    private static final String XSD_TAG_SIMPLETYPE = "xs:simpleType";
    private static final String XSD_TAG_ANNOTATION = "xs:annotation";
    private static final String XSD_TAG_RESTRICTION = "xs:restriction";
    private static final String XSD_TAG_APPINFO = "xs:appinfo";
    private static final String XSD_ATTR_STRING = "xs:string";
    private static final String XSD_ATTR_INTEGER = "xs:integer";
    private static final String XSD_XMLNS_PREFIX = "xmlns:xs";
    private static final String XSD_XMLNS_URL = "http://www.w3.org/2001/XMLSchema";
    private static final String BOOMI_XMLNS_PREFIX = "xmlns:b";
    private static final String BOOMI_XMLNS_URL = "http://www.boomi.com/connector/annotation";
    private static final String TAG_BOOMI_DATA_FORMAT = "b:dataFormat";
    private static final String TAG_BOOMI_FIELD_SPEC = "b:fieldSpec";
    private static final String ATTR_BOOMI_IGNORE_FOR_SORT = "ignoreForSort";
    private static final String ATTR_BOOMI_DATATYPE = "dataType";
    private static final String ATTR_BOOMI_FORMAT = "format";
    private static final String ATTR_BASE = "base";
    private static final String BOOMI_DATETIME_TYPE = "datetime";
    private static final String BOOMI_NUMBER_TYPE = "number";
    private static final String SALESFORCE_DATETIME_TYPE = "datetime";
    private static final String SALESFORCE_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final String SALESFORCE_DATE_TYPE = "date";
    private static final String SALESFORCE_DATE_FORMAT = "yyyy-MM-dd";
    private static final String SALESFORCE_ADDRESS_TYPE = "address";
    private static final String KEY_CHARACTER = "ch";
    private static final String KEY_DATETIME = "dtt";
    private static final String KEY_DATE = "dt";
    private static final String KEY_NUMBER = "nm";
    private static final String TAG_EXTENSION = "xs:extension";
    private static final String TAG_COMPLEX_CONTENT = "xs:complexContent";
    /**
     * Integer to specify the level of parent in the import hierarchy
     */
    protected int _parentDepth;

    /**
     * Integer to specify the level of children in the import hierarchy
     */
    protected int _childDepth;

    /**
     * the main schema element
     */
    protected Element _schema;

    /**
     * the main document responsible for creating elements
     */
    protected Document _doc;

    /**
     * HashSet used to maintain unique element definitions (used to mark visit for the DFS), used to avoid re-create the
     * same SObjects in the parent/child relationships instead of defining the same element multiple times
     */
    protected Set<String> _definedSet;

    /**
     * Controller responsible for retrieving metadata needed for the import operation
     */
    protected SObjectController _sobjectController;

    /**
     * Initializes the schema
     *
     * @param controller    controller to retrieve SObjects metadata from Salesforce
     * @param parentsDepth  Integer, will import parents up to level parentsDepth
     * @param childrenDepth Integer, will import children up to level childrenDepth
     */
    public XMLSchemaBuilder(SObjectController controller, long parentsDepth, long childrenDepth) {
        _sobjectController = controller;
        _parentDepth = (int) parentsDepth - 1;
        _childDepth = (int) childrenDepth - 1;

        // Creates the 'doc' will be used to create all schema elements
        _doc = DOMUtil.newDocument();

        // Creates the '_definedSet' will be used to store all the referenced SObjects in the DFS, to avoid duplicate
        // elements
        _definedSet = new HashSet<>();

        // Creates the main 'schema' will be used to create all schema definitions and elements
        _schema = _doc.createElement(XSD_TAG_SCHEMA);
        _schema.setAttribute(XSD_XMLNS_PREFIX, XSD_XMLNS_URL);
        _schema.setAttribute(BOOMI_XMLNS_PREFIX, BOOMI_XMLNS_URL);

        // generates the field types that will later be referenced in the schema
        defineSchemaFieldTypes();
    }

    /**
     * Generates ObjectDefinition used for importing the XML profile, will generate main SObject with its children and
     * their direct parents.<br>
     * <br>
     * As well as parents of the main SObject up to parentsDepth level of parents.<br>
     * <br>
     * Imports profile for any Operation as specified by the SObjectController initialization.
     *
     * @param sobjectName name of the main imported SObject
     * @return ObjectDefinition contains the generated XML profile
     */
    public ObjectDefinition generateSchema(String sobjectName) {
        ObjectDefinition def = new ObjectDefinition();
        def.setInputType(ContentType.XML);
        def.setOutputType(ContentType.XML);

        Element sobject = generateMainSObject(sobjectName);

        // append the generated SObject to the schema
        _schema.appendChild(sobject);

        def.setSchema(_schema);
        return def;
    }

    /**
     * The main function, generates the main XSD element representing the imported SObject, to be directly appended to
     * the _schema Element
     *
     * @param sobjectName main SObject name
     * @return Element to be appended as direct child to Schema Tag
     */
    protected abstract Element generateMainSObject(String sobjectName);

    /**
     * Creates and return XSD Complex Element contains 'baseTypeName' appended to it the 'extraElements' by referencing
     * the 'baseTypeName' not copying it.<br>
     * <br>
     * The returned element could either be used as a xs:complexType to be referenced as type, or directly appended
     * inside xs:element
     *
     * @param rootName      outer root element name of the complex Element
     * @param baseTypeName  name of complexType to be referenced
     * @param extraElements extra elements to be appended at the end of the fields, could be nested elements
     * @return Element contains list of fields and extra elements
     */
    protected Element generateComplexTypeExtendsComplexType(String rootName, String baseTypeName,
                                                            List<Element> extraElements) {
        Element sequence = _doc.createElement(XSD_TAG_SEQUENCE);

        if (extraElements != null) {
            for (Element sub : extraElements) {
                sequence.appendChild(sub);
            }
        }

        Element extension = _doc.createElement(TAG_EXTENSION);
        extension.setAttribute(ATTR_BASE, baseTypeName);
        extension.appendChild(sequence);

        Element complexContent = _doc.createElement(TAG_COMPLEX_CONTENT);
        complexContent.appendChild(extension);

        Element complexType = _doc.createElement(XSD_TAG_COMPLEXTYPE);
        complexType.setAttribute(ATTR_NAME, rootName);
        complexType.appendChild(complexContent);

        return complexType;
    }

    /**
     * Creates schema complex element contains fields and sub-objects.<br>
     * <br>
     * The returned element could either be used as a xs:complexType to be referenced as type, or directly appended
     * inside xs:element
     *
     * @param rootName      outer root element name of the complex Element
     * @param fields        list of fields of the element
     * @param extraElements extra elements to be appended at the end of the fields, could be nested elements
     * @return Element contains list of fields and extra elements
     */
    protected Element generateComplexType(String rootName, List<SObjectField> fields, List<Element> extraElements) {
        Element sequence = _doc.createElement(XSD_TAG_SEQUENCE);

        if (fields != null) {
            for (SObjectField field : fields) {
                sequence.appendChild(generateField(field));
            }
        }

        if (extraElements != null) {
            for (Element sub : extraElements) {
                sequence.appendChild(sub);
            }
        }

        Element complexType = _doc.createElement(XSD_TAG_COMPLEXTYPE);
        if (StringUtil.isNotBlank(rootName)) {
            complexType.setAttribute(ATTR_NAME, rootName);
        }
        complexType.appendChild(sequence);

        if (fields != null) {
            // has fields means a 'records' element set @Type attribute
            Element attributeType = _doc.createElement(TAG_ATTRIBUTE);
            attributeType.setAttribute("name", "type");
            complexType.appendChild(attributeType);
        }

        return complexType;
    }

    /**
     * Creates a specific field element, handles different field type formats
     *
     * @param sobjectField field model to be added
     * @return Element contains the schema field with its type
     */
    protected Element generateField(SObjectField sobjectField) {
        Element elementField = _doc.createElement(XSD_TAG_ELEMENT);
        elementField.setAttribute(ATTR_NAME, sobjectField.getName());
        elementField.setAttribute(ATTR_MINOCCURS, "0");
        if (SALESFORCE_ADDRESS_TYPE.equals(sobjectField.getType())) {
            elementField.setAttribute(ATTR_TYPE, KEY_CHARACTER);
        } else if (SALESFORCE_DATETIME_TYPE.equals(sobjectField.getType())) {
            elementField.setAttribute(ATTR_TYPE, KEY_DATETIME);
        } else if (SALESFORCE_DATE_TYPE.equals(sobjectField.getType())) {
            elementField.setAttribute(ATTR_TYPE, KEY_DATE);
        } else if (isNumericFieldType(sobjectField.getType())) {
            elementField.setAttribute(ATTR_TYPE, XSD_ATTR_STRING);
            elementField.appendChild(
                    generateFieldDefinition(KEY_NUMBER, BOOMI_NUMBER_TYPE, sobjectField.getPlatformFormat())
                            .getFirstChild());
        } else {
            elementField.setAttribute(ATTR_TYPE, KEY_CHARACTER);
        }
        return elementField;
    }

    /**
     * Returns true if the salesforce field type is numeric
     *
     * @param salesforceFieldType the type attribute in salesforce SObject metadata
     * @return true if the salesforce field type is numeric
     */
    private static boolean isNumericFieldType(String salesforceFieldType) {
        return "int".equals(salesforceFieldType) || "double".equals(salesforceFieldType) ||
               "currency".equals(salesforceFieldType) || "percent".equals(salesforceFieldType);
    }

    /**
     * Generates and append in the Schema the needed XML xs:simpleType definitions for all possible field types in the
     * schema so they will only be referenced instead of repeating type definitions
     */
    private void defineSchemaFieldTypes() {
        defineForReference(generateFieldDefinition(KEY_DATETIME, BOOMI_DATETIME_TYPE, SALESFORCE_DATETIME_FORMAT));

        defineForReference(generateFieldDefinition(KEY_DATE, BOOMI_DATETIME_TYPE, SALESFORCE_DATE_FORMAT));

        defineForReference(generateFieldDefinition(KEY_CHARACTER, null, null));

        defineForReference(generateIgnoreForSorts());

        defineForReference(generateComplexType(TAG_SALESFORCE_DONE, null, null));
        defineForReference(generateComplexType(TAG_SALESFORCE_SIZE, null, null));
    }

    /**
     * Generates an XML xs:simpleType to be used as 'definition' in the schema and referenced in the elements by
     * 'type=[fieldTypeName]'.<br>
     * <br>
     * Field contains Platform Character, Date/Time or Number field format with ignoreForSort option
     *
     * @param fieldTypeName the name to be referenced
     * @param fieldType     Platform annotation Character, Date, DateTime or Number
     * @param fieldFormat   Platform annotation format for the fieldType
     */
    protected Element generateFieldDefinition(String fieldTypeName, String fieldType, String fieldFormat) {
        // xs:simpleType wraps the field with name of fieldTypeName
        Element simpleType = _doc.createElement(XSD_TAG_SIMPLETYPE);
        simpleType.setAttribute(ATTR_NAME, fieldTypeName);

        // xs:annotation contains xs:appinfo
        Element annotation = _doc.createElement(XSD_TAG_ANNOTATION);
        Element appinfo = _doc.createElement(XSD_TAG_APPINFO);

        if (StringUtil.isNotBlank(fieldType)) {
            // Platform 'dataFormat' tag defines the Character, Date, DateTime or Number
            Element dataFormat = _doc.createElement(TAG_BOOMI_DATA_FORMAT);
            dataFormat.setAttribute(ATTR_BOOMI_DATATYPE, fieldType);
            dataFormat.setAttribute(ATTR_BOOMI_FORMAT, fieldFormat);
            appinfo.appendChild(dataFormat);
        }

        annotation.appendChild(appinfo);

        // xs:restriction required and specifies xs:string or xs:integer
        Element restriction = _doc.createElement(XSD_TAG_RESTRICTION);
        if (BOOMI_NUMBER_TYPE.equals(fieldType) && StringUtil.isBlank(fieldFormat)) {
            restriction.setAttribute(ATTR_BASE, XSD_ATTR_INTEGER);
        } else {
            restriction.setAttribute(ATTR_BASE, XSD_ATTR_STRING);
        }

        simpleType.appendChild(annotation);
        simpleType.appendChild(restriction);
        return simpleType;
    }

    /**
     * generates xs:annotation with the ignoreForSort fieldSpec
     *
     * @return Element the xs:annotation
     */
    protected Element generateIgnoreForSorts() {
        // xs:simpleType wraps the field with name of fieldTypeName
        Element simpleType = _doc.createElement(XSD_TAG_SIMPLETYPE);
        simpleType.setAttribute(ATTR_NAME, KEY_IGNORE_SORT);

        // xs:annotation contains xs:appinfo
        Element annotation = _doc.createElement(XSD_TAG_ANNOTATION);
        Element appinfo = _doc.createElement(XSD_TAG_APPINFO);

        // Platform 'fieldSpec' tag specifies the ignoreForSorts
        Element fieldSpec = _doc.createElement(TAG_BOOMI_FIELD_SPEC);
        fieldSpec.setAttribute(ATTR_BOOMI_IGNORE_FOR_SORT, "true");
        appinfo.appendChild(fieldSpec);
        annotation.appendChild(appinfo);

        simpleType.appendChild(annotation);
        return simpleType;
    }

    /**
     * Saves the typeElement in the _schema to be referenced later
     *
     * @param typeElement either XS:ComplexType or XS:SimpleType element
     */
    protected void defineForReference(Element typeElement) {
        _schema.appendChild(typeElement);
    }
}
