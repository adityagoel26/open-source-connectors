// Copyright (c) 2024 Boomi, LP.
package com.boomi.snowflake.util;

import com.boomi.connector.api.ConnectorException;

public final class SnowflakeOverrideConstants {

    private SnowflakeOverrideConstants() {
        throw new ConnectorException("Unable to instantiate class");
    }

    /** The Constant DATABASE. */
    public static final String DATABASE = "db";

    /** The Constant SCHEMA. */
    public static final String SCHEMA = "schema";

    /** The Constant ENABLECONNECTIONOVERRIDE. */
    public static final String ENABLECONNECTIONOVERRIDE= "enableConnectionOverride";

    /** The Constant for COPY COMMAND ERROR. */
    public static final String COPY_COMMAND_ERROR="Failed to execute Copy Into statement";

    /** The Constant for COPY DOUBLE QUOTE. */
    public static final String DOUBLE_QUOTE = "\"";

    /** The Constant for COPY SINGLE QUOTE. */
    public static final String SINGLE_QUOTE = "\'" ;

    /** The Constant for COPY DOT. */
    public static final String DOT = ".";

    /** The Constant PROP_ENABLE_POOLING. */
    public static final String PROP_ENABLE_POOLING = "enablePooling";

    /** The Constant PROP_ENABLE_POOLING. */
    public static final String BLANK_STRING = " ";


}
