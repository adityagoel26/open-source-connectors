## MongoDB Connector##
MongoDB connector allows you to transfer bulk/single data from/to any system by connecting to MongoDB server using MongoDB Java API and Dell Boomi Integration. Through this connector, you can use a Dell Boomi Integration process to perform operations such as GET, CREATE, QUERY, UPDATE, DELETE and UPSERT. MongoDB stores data in a flexible, polymorphic and extensible way.

The following are the values you recognize by using this connector

*	Easier way to convert the results from the database straight into domain objects.
*	Uses Mongo java driver whose API is consistent and also future-compatible.
*	Documents are stored as BSON data which are easy to transfer and are grouped together into collections.
*	We can perform multiple operations on the Database records using the APIs.

## Prerequisites ##
The following prerequisites are necessary to implement a connection to your account from Boomi Integration

*   Set the appropriate Java environment variables on your system, such as JAVA_HOME, CLASSPATH.
*   To use the connector and implement a connection to your MongoDB server from 
    Boomi Integration, ensure you have the following:
        *   Hostname or ip address for the MongoDB server.
        *   Deployed Atom on your local machine or host it with Boomi Integration.
            
## Building this connector project ##
To build this project, the following dependencies must be met

 * JDK 1.7 or above
 * Apache Maven 3.5.0
### Compiling ###
Once your JAVA_HOME points to your installation of JDK 1.8 (or above) and JAVA_HOME/bin and Apache maven are in your PATH, issue the following command to build the jar file:
```
  mvn install
```
or if you don't want to run the unit tests, then run 
```
  mvn install -DskipTests
``` 
### Running the unit tests ###
To run the unit tests, please use below command 
``` 
  mvn install 
```
It will run the unit tests and build the jar file. Both the CAR file and the Test Reports will be available inside target folder.

## Supported Operations ##
The MongoDB operations define how to interact with your MongoDB server and represent a specific action (Get, Update, Delete, Create, Upsert, and Query). Create a separate operation component for each action/object combination that your integration requires. The MongoDB connector supports the following operations:

Inbound: Get, Query.
Outbound: Update, Delete, Create, Upsert.

## Operation Details ##

### Get ###
Get is an inbound action for which you provide the ObjectID of the record for which you want to retrieve details from the Server.

### Update ###
The update is an outbound action where you can update the existing records in the MongoDB either in bulk or a single record by specifying Object/Record ID(s). If the ID exists, the connector automatically replace the existing record in database and performs an update operation and If the ID does not exist, the connector will not process the record.

### Delete ###
Delete is an outbound action where you can delete the existing records in the MongoDB either in bulk or a single record by specifying Object/Record ID(s). If ID exists, the connector will automatically delete the existing record and If ID do not exist, the connector will ignore the record.

### Create ###
Create is an outbound action where you can Upload and add a new document to the MongoDB by specifying Record ID and Partition Key.

### Upsert ###
Upsert is an outbound action to either update or create a new record in MongoDB collection. This operation can be performed either in bulk or a single record by specifying Object/Record ID(s). If ID already exists, the connector automatically performs an update operation and If the ID does not exist, the connector will create a new record.

### Query ###
The query is an inbound action that looks up objects in MongoDB collection and returns zero to many object records. After you select the Query action and use the Import Wizard, the operation component page contains configuration options to add filters and set parameters to limit the results. You can refine the query on the Filters tab and define expressions to create required query logic.

## User Guide ##


## Additional resources ##
* [MongoDB Official Documentation](https://docs.mongodb.com/)