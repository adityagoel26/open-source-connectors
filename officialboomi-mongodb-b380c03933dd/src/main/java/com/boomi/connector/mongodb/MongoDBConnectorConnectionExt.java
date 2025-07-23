// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.mongodb;

import java.io.IOException;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.mongodb.constants.DataTypes;
import com.boomi.connector.mongodb.constants.MongoDBConstants;
import com.boomi.connector.mongodb.exception.MongoDBConnectException;
import com.boomi.connector.mongodb.util.DocumentUtil;
import com.boomi.connector.mongodb.util.JsonSchemaUtil;
import com.boomi.connector.mongodb.util.ProfileUtils;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

public class MongoDBConnectorConnectionExt {

    private MongoDBConnectorConnectionExt() {
        throw new IllegalStateException("Extension of connector Connection class ");
    }

    /**
     * Find the document for the provided Id.
     * Gets the value from the platform and formats them as per the data type.
     *
     * @param mongoDBConnectorConnection the mongoDB connector connection
     * @param collectionName             the collection name
     * @param objectId                   the object id
     * @param projectionFields           the projection fields
     * @return the document
     * @throws MongoDBConnectException the mongo DB connect exception
     */
    public static Document findDocumentById(MongoDBConnectorConnection mongoDBConnectorConnection,
            String collectionName, String objectId, String projectionFields, String dataType)
            throws MongoDBConnectException {
        Document doc;
        Object val = null;
        Bson idFilter;
        FindIterable<Document> query;
        Bson finalFilter;
        AtomConfig atomConfig = mongoDBConnectorConnection.getContext().getConfig();
        if (ObjectId.isValid(objectId)) {
            MongoCollection<Document> coll = mongoDBConnectorConnection.getCollection(collectionName);
            idFilter = Filters.in(MongoDBConstants.ID_FIELD_NAME, new ObjectId(objectId), objectId);
            finalFilter = DocumentUtil.buildFilterWithMaxDocumentSize(atomConfig, idFilter);
            query = coll.find(finalFilter);
            doc = mongoDBConnectorConnection.setProjectionsInQuery(query, projectionFields).first();
            exceptionOnNullDoc(doc);
            return doc;
        } else if (!ObjectId.isValid(objectId) && dataType == null) {
            val = getNonHexaDecDocument(mongoDBConnectorConnection, objectId, val);
        } else {
            switch (dataType) {
                case DataTypes.INTEGER:
                    val = mongoDBConnectorConnection.formatInputForNumberType(dataType, objectId);
                    break;
                case DataTypes.DECIMAL_128:
                    val = mongoDBConnectorConnection.formatInputForDecimal128Type(dataType, objectId);
                    break;
                case DataTypes.BOOLEAN:
                    val = mongoDBConnectorConnection.formatInputForBooleanType(dataType, objectId);
                    break;
                case DataTypes.DOUBLE:
                    val = mongoDBConnectorConnection.formatInputForDoubleType(dataType, objectId);
                    break;
                case DataTypes.LONG:
                    val = mongoDBConnectorConnection.formatInputForLongType(dataType, objectId);
                    break;
                case DataTypes.NULL:
                    val = mongoDBConnectorConnection.formatInputForNullType(dataType, objectId);
                    break;
                case DataTypes.OBJECT_ID:
                case DataTypes.NONE:
                    val = mongoDBConnectorConnection.formatInputForStringType(dataType, objectId);
                    break;
                case DataTypes.BINARY_DATA:
                case DataTypes.STRING:
                case DataTypes.DATE:
                case DataTypes.JAVA_SCRIPT:
                case DataTypes.TIMESTAMP:
                    val = objectId;
                    break;
                default:
                    throw new MongoDBConnectException("Invalid value type-");
            }
        }
        MongoCollection<Document> coll = mongoDBConnectorConnection.getCollection(collectionName);
        idFilter = Filters.in(MongoDBConstants.ID_FIELD_NAME, val, val);
        finalFilter = DocumentUtil.buildFilterWithMaxDocumentSize(atomConfig, idFilter);
        query = coll.find(finalFilter);
        doc = mongoDBConnectorConnection.setProjectionsInQuery(query, projectionFields).first();
        exceptionOnNullDoc(doc);
        return doc;
    }

    private static Object getNonHexaDecDocument(MongoDBConnectorConnection mongoDBConnectorConnection, String objectId, Object val) throws MongoDBConnectException {
        if ("null".equals(objectId))
        {
        	val = mongoDBConnectorConnection.formatInputForNullType(null, objectId);
        }else {
        val = getNotNullValues(mongoDBConnectorConnection, objectId, val);
        }
        return val;
    }


	private static Object getNotNullValues(MongoDBConnectorConnection mongoDBConnectorConnection, String objectId,
			Object val) throws MongoDBConnectException {
		Document doc;
		String jsonSchema;
		ProfileUtils profileUtils;
		doc = Document.parse(objectId);
        jsonSchema = JsonSchemaUtil.createJsonSchema(doc);
        profileUtils = new ProfileUtils(jsonSchema);
        Object properties = doc.get(MongoDBConstants.ID_FIELD_NAME);
        if (jsonSchema.contains("$numberLong")) {
            if (properties instanceof Long) {
                String longVal = null;
                val = mongoDBConnectorConnection.formatInputForLongType(longVal, doc.get("_id").toString());
            }

        } else if (jsonSchema.contains("$numberDecimal")) {
            if (properties instanceof Decimal128) {
                String decimal128Val = null;
                val = mongoDBConnectorConnection.formatInputForDecimal128Type(decimal128Val, doc.get("_id").toString());
            }

        } else if (jsonSchema.contains("$numberDouble")) {
            if (properties instanceof Double) {
                String doubleVal = null;
                val = mongoDBConnectorConnection.formatInputForDoubleType(doubleVal, doc.get("_id").toString());
            }

        } else {
            try {
                String result = profileUtils.getType(MongoDBConstants.ID_FIELD_NAME);
                switch (result.toUpperCase()) {
                    case DataTypes.INTEGER:
                    case DataTypes.NUMBER:
                        val = mongoDBConnectorConnection.formatInputForNumberType(result, doc.get("_id").toString());
                        break;
                    case DataTypes.BOOLEAN:
                        val = mongoDBConnectorConnection.formatInputForBooleanType(result, doc.get("_id").toString());
                        break;
                    case DataTypes.NULL:
                        val = mongoDBConnectorConnection.formatInputForNullType(result, doc.get("_id", "NULL"));
                        break;
                    case DataTypes.OBJECT:
                        val = mongoDBConnectorConnection.formatInputForStringType(result, doc.get("_id").toString());
                        break;
                    case DataTypes.BINARY_DATA:
                    case DataTypes.STRING:
                    case DataTypes.DATE:
                    case DataTypes.JAVA_SCRIPT:
                    case DataTypes.TIMESTAMP:
                        val = doc.get("_id");
                        break;
                    default:
                        throw new MongoDBConnectException("Invalid value type-");
                }
            } catch (IOException e) {
                throw new MongoDBConnectException(e.getMessage());
            }
        }
		return val;
	}

    private static void exceptionOnNullDoc(Document doc) throws MongoDBConnectException {
        if (doc == null) {
            throw new MongoDBConnectException("id is not in collection");
        }
    }
}