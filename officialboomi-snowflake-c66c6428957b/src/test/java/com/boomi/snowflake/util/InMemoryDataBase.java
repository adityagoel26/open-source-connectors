// Copyright (c) 2024 Boomi, LP.
package com.boomi.snowflake.util;

import com.boomi.util.LogUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.StringJoiner;
import java.util.logging.Logger;

/**
 * Utility class for creating and managing DB and Schema.
 * Provides static methods to configure DB and Schema as needed.
 */
public class InMemoryDataBase {
    private static final Logger LOG = LogUtil.getLogger(InMemoryDataBase.class);
    private final String url;
    private final String user;
    private final String password;
    private final String schema;
    private final String table;
    private final List<String> fields;
    private final String insertDataSQL;
    private final String querySQL;
    private  Connection connection;

    /**
     * Constructs an `InMemoryDataBase` instance with the specified configuration.
     *
     * @param url           the JDBC URL for the in-memory database
     * @param user          the username for database authentication
     * @param password      the password for database authentication
     * @param schema        the schema to be used in the database
     * @param table         the name of the table to be created in the database
     * @param fields        a list of column definitions for creating the table
     * @param insertDataSQL a SQL statement for inserting sample data into the table
     * @param querySQL      a SQL statement for querying data from the table
     */
    public InMemoryDataBase(String url, String user, String password, String schema, String table,
            List<String> fields, String insertDataSQL, String querySQL) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.schema = schema;
        this.table = table;
        this.fields = fields;
        this.insertDataSQL = insertDataSQL;
        this.querySQL = querySQL;
    }

    /**
     * Generates a SQL statement to create a schema if it does not already exist.
     * Includes the specified schema name in the statement.
     *
     * @param schema the name of the schema to be created
     * @return a SQL statement to create the schema if it does not exist
     */
    private static String createSchema(String schema) {
        return ";INIT=CREATE SCHEMA IF NOT EXISTS " + schema + "\\;";
    }

    /**
     * Generates a SQL statement to set the current schema.
     *
     * @param schema the name of the schema to be set
     * @return a SQL statement to set the current schema
     */
    private static String setSchema(String schema) {
        return "SET SCHEMA " + schema;
    }

    /**
     * Generates a SQL statement to create a table with the specified name and fields.
     * Ensures the table is created if it does not already exist.
     *
     * @param tableName the name of the table to be created
     * @param fields    a list of column definitions for the table (e.g., "id INT", "name VARCHAR(255)")
     * @return a SQL statement to create the table
     */
    public static String createTableSQL(String tableName, List<String> fields) {
        StringJoiner fieldJoiner = new StringJoiner(", ");
        for (String field : fields) {
            fieldJoiner.add(field);
        }
        return String.format("CREATE TABLE IF NOT EXISTS %s (%s)", tableName, fieldJoiner);
    }

    /**
     * Connects to the database, creates the schema and table, inserts data, and performs a query.
     * Handles database operations and cleanup with exception handling.
     */
    public void createDBandSCHEMA() {
        try {
            Statement statement;
            // Step 1: Load the H2 driver (Optional in modern versions)
            Class.forName("org.h2.Driver");
            // Step 2 : Establishes a database connection with the specified URL, schema, user, and password.
            connection = DriverManager.getConnection(url + createSchema(schema)
                    + setSchema(schema), user, password);
            // Step 3: Create a statement
            statement = connection.createStatement();
            // Step 4: Create a table
            String createTableSQL = createTableSQL(table, fields);
            statement.execute(createTableSQL);
            // Step 5: Insert data into the table
            statement.executeUpdate(insertDataSQL);
            // Step 6: Query the data
            ResultSet resultSet = statement.executeQuery(querySQL);
            // Clean up (optional)
            resultSet.close();
        } catch (ClassNotFoundException | SQLException e) {
            LOG.info("Error connecting to the database: " + e.getMessage());
        }
    }

    /**
     * Retrieves the current database connection.
     * @return the current database connection
     */
    public Connection getConnection() {
        return connection;
    }
}