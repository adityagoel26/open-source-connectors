// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.util;

public final class JMSConstants {

    //// Container properties
    public static final String PROPERTY_DOCUMENT_SIZE_THRESHOLD = "com.boomi.connector.jmssdk.max.message.size";
    // Shared
    public static final String PROPERTY_JMS_VERSION = "version";

    //// Connection properties
    public static final String PROPERTY_SERVER_TYPE = "server_type";
    public static final String PROPERTY_USE_AUTHENTICATION = "authentication";
    public static final String PROPERTY_USERNAME = "username";
    public static final String PROPERTY_PASSWORD = "password";

    // Generic JNDI
    public static final String PROPERTY_JNDI_LOOKUP_FACTORY = "jndi_lookup_factory";
    public static final String PROPERTY_INITIAL_CONTEXT_FACTORY = "initial_context_factory";
    public static final String PROPERTY_PROVIDER_URL = "provider_url";
    public static final String PROPERTY_JMS_PROPERTIES = "jms_properties";

    //Oracle AQ
    public static final String PROPERTY_JDBC_URL = "jdbc_url";

    // Common to all operations
    public static final String PROPERTY_DESTINATION = "destination";
    public static final String PROPERTY_DESTINATION_TYPE = "destination_type";
    public static final String PROPERTY_USE_TRANSACTION = "use_transaction";

    // Send Operation
    public static final String PROPERTY_TRANSACTION_BATCH_SIZE = "transaction_batch_size";
    public static final String PROPERTY_CORRELATION_ID = "correlation_id";
    public static final String PROPERTY_MESSAGE_TYPE = "message_type";
    public static final String PROPERTY_PRIORITY = "priority";
    public static final String PROPERTY_REPLY_TO = "reply_to";
    public static final String PROPERTY_CUSTOM_OPERATION_PROPERTIES = "jms_operation_properties";
    public static final String PROPERTY_TIME_TO_LIVE = "time_to_live";

    // Get Operation
    public static final String PROPERTY_USE_DURABLE_SUBSCRIPTION = "use_durable_subscription";
    public static final String PROPERTY_SUBSCRIPTION_NAME = "subscription_name";
    public static final String PROPERTY_RECEIVE_MODE = "receive_mode";
    public static final String PROPERTY_NUMBER_OF_MESSAGES = "number_of_messages";
    public static final String PROPERTY_MAXIMUM_NUMBER_OF_MESSAGES = "maximum_number_of_messages";
    public static final String PROPERTY_RECEIVE_TIMEOUT = "receive_timeout";
    public static final String PROPERTY_MESSAGE_SELECTOR = "message_selector";

    // Listen Operation
    public static final String PROPERTY_MAX_CONCURRENT_EXECUTIONS = "max_concurrent_executions";
    public static final String PROPERTY_SINGLETON_LISTENER = "singleton_listener";
    public static final String PROPERTY_DELIVERY_POLICY = "delivery_policy";
    public static final String DELIVERY_POLICY_AT_LEAST_ONCE = "AT_LEAST_ONCE";
    public static final String DELIVERY_POLICY_AT_MOST_ONCE = "AT_MOST_ONCE";

    // Tracked Properties
    public static final String TRACKED_PROPERTY_CORRELATION_ID = "correlation_id";
    public static final String TRACKED_PROPERTY_DESTINATION = "destination";
    public static final String TRACKED_PROPERTY_MESSAGE_ID = "message_id";
    public static final String TRACKED_PROPERTY_MESSAGE_TYPE = "message_type";
    public static final String TRACKED_PROPERTY_MESSAGE_CLASS = "message_class";
    public static final String TRACKED_PROPERTY_PRIORITY = "priority";
    public static final String TRACKED_PROPERTY_REDELIVERED = "redelivered";
    public static final String TRACKED_PROPERTY_REPLY_TO = "reply_to";
    public static final String TRACKED_PROPERTY_EXPIRATION_TIME = "expiration_time";
    public static final String TRACKED_PROPERTY_GROUP = "custom_properties";
    public static final String DYNAMIC_DESTINATION_ID = "dynamic_destination";
    public static final String OP_PROP_ENTITY_FILTER = "entityFilter";
    public static final String WEBSPHERE_HOST_NAME = "websphere_host_name";
    public static final String WEBSPHERE_HOST_PORT = "websphere_host_port";
    public static final String WEBSPHERE_QUEUE_MANAGER = "websphere_queue_manager";
    public static final String WEBSPHERE_CHANNEL = "websphere_channel";
    public static final String WEBSPHERE_HOST_LIST = "websphere_host_list";
    public static final String WEBSPHERE_USE_SSL = "websphere_use_ssl";
    public static final String WEBSPHERE_SSL_SUITE_OPTION = "websphere_ssl_suite_option";
    public static final String WEBSPHERE_SSL_SUITE_TEXT = "websphere_ssl_suite_text";

    private JMSConstants() {
    }

    public enum JMSVersion {
        V2_0, V1_1
    }

    public enum ServerType {
        GENERIC_JNDI,
        ACTIVEMQ_CLASSIC,
        ACTIVEMQ_ARTEMIS,
        ORACLE_AQ,
        ORACLE_AQ_WEBLOGIC,
        SONICMQ,
        WEBSPHERE_MQ_SINGLE,
        WEBSPHERE_MQ_MULTI_INSTANCE
    }
}
