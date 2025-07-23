// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.util;

import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;

/**
 * Utility class for {@link ObjectMapper}. Unifies {@link ObjectMapper} configuration in DBv2 project.
 */
public class DBv2JsonUtil {

    private static final ObjectMapper OBJECT_MAPPER = JSONUtil.getDefaultObjectMapper().disable(
            MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS).disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    /**
     * This mapper deserializes JSON floating point numbers into {@link java.math.BigDecimal} if only generic type
     * description (either Object or Number, or within untyped {@link java.util.Map} or {@link java.util.Collection} context) is
     * available.
     */
    private static final ObjectMapper BIG_DECIMAL_OBJECT_MAPPER = JsonMapper.builder().disable(
            MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS).disable(SerializationFeature.FAIL_ON_EMPTY_BEANS).configure(
            JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true).configure(
            DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true).build();

    private DBv2JsonUtil()
    {
        // No instances needed.
    }

    /**
     *
     * @return Returns basic {@link ObjectMapper}.
     */
    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }

    /**
     * This mapper deserializes JSON floating point numbers into {@link java.math.BigDecimal} if only generic type
     * description (either Object or Number, or within untyped {@link java.util.Map} or {@link java.util.Collection} context) is
     * available.
     *
     * @return ObjectMapper with BigDecimal precision
     */
    public static ObjectMapper getBigDecimalObjectMapper() {
        return BIG_DECIMAL_OBJECT_MAPPER;
    }

    /**
     * Cheap to construct and can be configured to be used per-call basis.
     *
     * @return {@link ObjectReader} with basic config
     */
    public static ObjectReader getObjectReader() {
        return OBJECT_MAPPER.reader();
    }

    /**
     * Cheap to construct and can be configured to be used per-call basis.
     *
     * @return {@link ObjectWriter}
     */
    public static ObjectWriter getObjectWriter() {
        return OBJECT_MAPPER.writer();
    }
}
