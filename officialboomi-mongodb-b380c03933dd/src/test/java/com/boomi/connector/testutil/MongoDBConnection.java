// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.testutil;

import com.boomi.connector.mongodb.MongoDBConnectorConnection;
import com.mongodb.client.MongoDatabase;

public interface MongoDBConnection {

    public MongoDatabase getMongoDBConnection();
}
