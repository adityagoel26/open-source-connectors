// Copyright (c) 2022 Boomi, LP
package com.boomi.connector.mongodb;

import com.boomi.connector.api.Connector;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.ConnectorTestContext;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MongoDBCredentials extends ConnectorTestContext {

    private static final Logger logger = Logger.getLogger(MongoDBCredentials.class.getName());

    public MongoDBCredentials() {
        super();
        Properties props = new Properties();
        try (FileInputStream file = new FileInputStream("src/test/resources/mongo.properties")) {
            props.load(file);
        } catch (FileNotFoundException e) {
            logger.log(Level.INFO, "NoFile", e);
        } catch (IOException e) {
            logger.log(Level.INFO, "Input not proper", e);
        }
        this.addConnectionProperty("username", props.getProperty("username"));
        this.addConnectionProperty("password", props.getProperty("password"));
        this.addConnectionProperty("host", props.getProperty("host"));
        this.addConnectionProperty("port", props.getProperty("port"));
    }


    public MongoDBCredentials(OperationType operationType) {
        this();
        setOperationType(operationType);
    }

    @Override
    protected Class<? extends Connector> getConnectorClass() {
        return null;
    }

}