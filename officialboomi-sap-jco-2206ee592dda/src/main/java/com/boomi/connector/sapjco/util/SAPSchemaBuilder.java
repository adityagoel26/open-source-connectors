// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sapjco.util;

import java.util.logging.Logger;

import com.boomi.util.LogUtil;
import com.boomi.util.SchemaBuilder;
import com.boomi.util.SchemaUtil;
import com.boomi.util.StringUtil;
import com.sap.conn.jco.JCoMetaData;

/**
 * @author kishore.pulluru
 *
 */
public abstract class SAPSchemaBuilder {
    private static final Logger LOG = Logger.getLogger(SAPSchemaBuilder.class.getName());
    private static final Object FIELD_MARK = new Object();
    private static final Object CT_MARK = new Object();
    private static final String VALUE = "value";

    protected SAPSchemaBuilder() {
    }

    /**
     * Appends primitive datatype to the xsd
     */
    protected static void appendField(SchemaBuilder builder, int type, String name, int length, String minOccur,
            String maxOccur, String documentation) {
        String cleanedName = SAPUtil.escape(name);

        if (StringUtil.isEmpty(cleanedName)) {
            LogUtil.fine(LOG,
                    "SAP Field with empty name for JCoMetaData type: %s and description: %s, will not be added to the"
                            + " profile.",
                    type, documentation);
        }
        switch (type) {
        case JCoMetaData.TYPE_CHAR:
        case JCoMetaData.TYPE_STRING:
            builder.mark(FIELD_MARK);
            builder.appendSchemaElement(cleanedName, null);
            builder.setOccursAttributes(minOccur, maxOccur);
            appendDocumentation(builder, documentation);
            builder.appendSimpleType(null);
            builder.appendRestriction(SchemaUtil.TYPE_STRING);
            builder.append(SchemaUtil.SCHEMA_NAMESPACE, "minLength", null).setAttribute(VALUE, "0").toParent();
            builder.append(SchemaUtil.SCHEMA_NAMESPACE, "maxLength", null).setAttribute(VALUE,
                    String.valueOf(length));
            builder.toMark(FIELD_MARK);
            break;
        case JCoMetaData.TYPE_FLOAT:
        case JCoMetaData.TYPE_BCD:
        	builder.mark(FIELD_MARK);
            builder.appendSchemaElement(cleanedName, null);
            builder.setOccursAttributes(minOccur, maxOccur);
            appendDocumentation(builder, documentation);
            builder.appendSimpleType(null);
            builder.appendRestriction(SchemaUtil.TYPE_DECIMAL);
            builder.append(SchemaUtil.SCHEMA_NAMESPACE, "totalDigits", null).setAttribute(VALUE, String.valueOf(length)).toParent();
            builder.toMark(FIELD_MARK);
            break;
        case JCoMetaData.TYPE_NUM:
        case JCoMetaData.TYPE_INT:
        case JCoMetaData.TYPE_INT1:
        case JCoMetaData.TYPE_INT2:
        	builder.mark(FIELD_MARK);
            builder.appendSchemaElement(cleanedName, null);
            builder.setOccursAttributes(minOccur, maxOccur);
            appendDocumentation(builder, documentation);
            builder.appendSimpleType(null);
            builder.appendRestriction(SchemaUtil.TYPE_INTEGER);
            builder.append(SchemaUtil.SCHEMA_NAMESPACE, "totalDigits", null).setAttribute(VALUE, String.valueOf(length)).toParent();
            builder.toMark(FIELD_MARK);
            break;
        case JCoMetaData.TYPE_DATE:
        case JCoMetaData.TYPE_TIME:
            builder.appendSchemaElement(cleanedName, SchemaUtil.TYPE_DATETIME);
            builder.setOccursAttributes(minOccur, maxOccur);
            appendDocumentation(builder, documentation);
            builder.toParent();
            break;
        case JCoMetaData.TYPE_BYTE:
            builder.appendSchemaElement(cleanedName, SchemaUtil.SCHEMA_PREFIX + SchemaUtil.PREFIX_SEPARATOR + "byte");
            builder.setOccursAttributes(minOccur, maxOccur);
            appendDocumentation(builder, documentation);
            builder.toParent();
            break;
        default:
            break;
        }
    }

    /**
     * Appends complex datatype to the xsd
     */
    protected static void appendComplexField(SchemaBuilder builder, int type, String name, String minOccurs,
            String recType, JCoMetaData recMetadata, String documentation) {
        String cleanedName = SAPUtil.escape(name);
        String cleanedType = SAPUtil.escape(recType);

        if (StringUtil.isEmpty(cleanedName)) {
            LogUtil.fine(LOG,
                    "SAP Field with empty name for JCoMetaData type: %s and description: %s, will not be added to the"
                            + " profile.",
                    type, documentation);
        }
        switch (type) {
        case JCoMetaData.TYPE_STRUCTURE:
            builder.mark(CT_MARK);
            builder.appendSchemaElement(cleanedName, cleanedType);
            builder.setOccursAttributes(minOccurs, SchemaUtil.OCCURS_ONE);
            appendDocumentation(builder, documentation);
            builder.toMark(CT_MARK);
            addComplexTypeDefinition(builder, recMetadata, cleanedType);
            break;
        case JCoMetaData.TYPE_TABLE:
            builder.mark(CT_MARK);
            builder.appendSchemaElement(cleanedName, null);
            builder.setOccursAttributes(minOccurs, SchemaUtil.OCCURS_ONE);
            appendDocumentation(builder, documentation);
            builder.appendComplexType(null);
            builder.appendSequence();
            builder.appendSchemaElement("item", cleanedType);
            builder.setOccursAttributes(SchemaUtil.OCCURS_ZERO, SchemaUtil.OCCURS_UNBOUNDED);
            builder.toMark(CT_MARK);
            addComplexTypeDefinition(builder, recMetadata, cleanedType);
            break;
        default:
            break;
        }
    }

    /**
     * Adds a complex type definition to the xsd
     */
    private static void addComplexTypeDefinition(SchemaBuilder builder, JCoMetaData metadata, final String typeName) {
        // Create a type marker to deal with nested complex types
        Object typeMarker = new Object() {
            public String toString() {
                return typeName + "Marker";
            }
        };
        builder.mark(typeMarker);
        builder.toRoot();
        builder.appendComplexType(typeName);
        builder.appendSequence();
        for (int i = 0; i < metadata.getFieldCount(); i++) {
            if (metadata.isStructure(i) || metadata.isTable(i)) {
                appendComplexField(builder, metadata.getType(i), metadata.getName(i), SchemaUtil.OCCURS_ZERO,
                        metadata.getRecordTypeName(i), metadata.getRecordMetaData(i), metadata.getDescription(i));
            }
            else {
                appendField(builder, metadata.getType(i), metadata.getName(i), metadata.getLength(i),
                        SchemaUtil.OCCURS_ZERO, SchemaUtil.OCCURS_ONE, metadata.getDescription(i));
            }
        }
        builder.toMark(typeMarker);
    }

    /**
     * Appends documentation to the element
     */
    protected static void appendDocumentation(SchemaBuilder builder, String documentation) {
        builder.appendAnnotation();
        builder.append(SchemaUtil.SCHEMA_NAMESPACE, "documentation", documentation);
        builder.toParent().toParent();
    }
    
    
    
   
}
