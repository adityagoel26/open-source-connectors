// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.model.oracleaq;

import oracle.jdbc.OracleTypes;
import oracle.jdbc.rowset.OracleSerialBlob;
import com.boomi.connector.api.ConnectorException;
import com.boomi.util.Base64Util;
import com.boomi.util.ExtSimpleDateFormat;
import com.boomi.util.StreamUtil;

import javax.sql.rowset.serial.SerialBlob;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

/**
 * This class is used by {@link AQObjectFactory} to convert Oracle data from an object representation to a string
 * representation suitable for inclusion in an XML document and back from the string representation to object suitable
 * for an attribute in an Oracle {@link Struct}. Simple types, such as {@link OracleTypes#VARCHAR} and
 * {@link OracleTypes#NUMERIC}, do not require a conversion and the data will be returned unaltered.
 */
public class AQDataConverter {

    private final SimpleDateFormat _dateFormat = new ExtSimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    /**
     * This method converts the specifed data from a string representation suitabale for XML to an object representation
     * suitable for a {@link Struct} attribute.
     *
     * @param data     string value of the data to convert
     * @param metaData {@link AQStructMetaData} describing the type
     * @param conn     Oracle connection used to create instances of certain object types (specifically blobs and clobs,
     *                 {@link SerialBlob} and {@link OracleSerialBlob} and the clob equivalents do not work)
     * @return the object representation of the data
     */
    public Object fromString(String data, AQStructMetaData metaData, Connection conn) {
        try {
            if (data != null) {
                if (metaData.isDateTimeType()) {
                    return new Timestamp(_dateFormat.parse(data).getTime());
                } else if (isBlobType(metaData)) {
                    Blob blob = conn.createBlob();
                    blob.setBytes(1, Base64Util.decodeToBytes(data));
                    return blob;
                } else if (isRawType(metaData)) {
                    return Base64Util.decodeToBytes(data);
                } else if (isClobType(metaData)) {
                    Clob clob = isNClobType(metaData) ? conn.createNClob() : conn.createClob();
                    clob.setString(1, data);
                    return clob;
                } else if (isXmlType(metaData)) {
                    SQLXML xml = conn.createSQLXML();
                    xml.setString(data);
                    return xml;
                }
            }
            return data;
        } catch (Exception e) {
            throw new ConnectorException(e);
        }
    }

    /**
     * This method converts the specifed data from an object representation suitable for a {@link Struct} attribute to a
     * string represenation suitable for XML.
     *
     * @param data     object value of the data to convert
     * @param metaData {@link AQStructMetaData} describing the type
     * @return the string representation of the data
     */
    public String fromObject(Object data, AQStructMetaData metaData) {
        try {
            if (data == null) {
                return null;
            } else if (metaData.isDateTimeType()) {
                return _dateFormat.format(data);
            } else if (isRawType(metaData)) {
                return Base64Util.encodeToString((byte[]) data);
            } else if (isBlobType(metaData)) {
                return Base64Util.encodeToString(((Blob) data).getBinaryStream());
            } else if (isClobType(metaData)) {
                return StreamUtil.toString(((Clob) data).getCharacterStream());
            } else if (isXmlType(metaData)) {
                return ((SQLXML) data).getString();
            }
            return data.toString();
        } catch (Exception e) {
            throw new ConnectorException(e);
        }
    }

    private static boolean isXmlType(AQStructMetaData metaData) {
        return OracleTypes.SQLXML == metaData.getType() || (OracleTypes.OPAQUE == metaData.getType()
                && metaData.getTypeName().endsWith("XMLTYPE"));
    }

    private static boolean isNClobType(AQStructMetaData metaData) {
        return OracleTypes.NCLOB == metaData.getType();
    }

    private static boolean isClobType(AQStructMetaData metaData) {
        return OracleTypes.CLOB == metaData.getType() || isNClobType(metaData);
    }

    private static boolean isBlobType(AQStructMetaData metaData) {
        return metaData.getType() == OracleTypes.BLOB;
    }

    private static boolean isRawType(AQStructMetaData metaData) {
        return metaData.getType() == OracleTypes.RAW;
    }
}
