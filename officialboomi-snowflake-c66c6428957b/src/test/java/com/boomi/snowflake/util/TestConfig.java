package com.boomi.snowflake.util;

import com.boomi.connector.api.ConnectorException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class TestConfig {
    private static final String URL = "jdbc:h2:mem:TEST_SF_DB";
    private static final String USER = "sa";
    private static final String PASSWORD = "pwd";
    private static Connection h2Connection;
    /**
     * Creates a database schema and table, then inserts sample data.
     * Initializes an in-memory database with specified schema, table, fields, and SQL statements.
     */
    public static void createDbAndSchema(String schema, String table) {
        List<String> fields = Arrays.asList("id INT AUTO_INCREMENT PRIMARY KEY", "name VARCHAR(255)",
                "salary DECIMAL(10, 2)");
        String insertDataSQL = getInsertStatement(schema, table);
        String querySQL = "SELECT * FROM DEV.employees";
        InMemoryDataBase inMemoryDataBase = new InMemoryDataBase(URL, USER, PASSWORD, schema, table, fields,
                insertDataSQL, querySQL);
        inMemoryDataBase.createDBandSCHEMA();
        h2Connection= inMemoryDataBase.getConnection();
    }
    /**
     * Generates an SQL insert statement based on the provided schema and table name.
     * Inserts different sets of data depending on whether the schema is "DEV" or not.
     */
    private static String getInsertStatement(String schema, String table) {
        String insertDataSQL;
        if (schema.equals("DEV")) {
            insertDataSQL = "INSERT INTO " + table + " (name, salary) VALUES " + "('John Doe', 50000.00),"
                    + "('Jane Smith', 60000.00)," + "('Alice Johnson', 75000.00)";
        } else {
            insertDataSQL = "INSERT INTO " + table + " (name, salary) VALUES " + "('James Cameron', 50000.00),"
                    + "('Steven Spielberg', 60000.00)," + "('Martin Scorsese', 75000.00)";
        }
        return insertDataSQL;
    }
    /**
     * Returns the H2 database connection instance.
     * @return the H2 connection
     */
    public static Connection getH2Connection() {
        return h2Connection;
    }
    /**
     * Closes the H2 database connection if it is open.
     * @throws RuntimeException if an SQL error occurs during closing
     */
    public static void closeConnection(){
        try {
            if(null != h2Connection && !h2Connection.isClosed()){
                h2Connection.close();
            }
        } catch (SQLException e) {
            throw new ConnectorException(e);
        }
    }

    public static String getJdbcUrl() {
        return URL;
    }

    public static String getUsername() {
        return USER;
    }

    public static String getPassword() {
        return PASSWORD;
    }
}

