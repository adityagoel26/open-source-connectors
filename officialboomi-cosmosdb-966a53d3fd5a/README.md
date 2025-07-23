# Microsoft Azure Cosmos DB connector #

Microsoft Azure Cosmos DB connector allows you to transfer data from/to Microsoft Azure Cosmos DB server, using Dell Boomi Integrations and REST APIs. These REST APIs provide access to Microsoft Azure Cosmos DB resources to Get, Update, Delete, Create, Upsert and Query documents from Collections and Databases.

The following are the values you recognize by using this connector

* Simplifies Cosmos DB database Connection
* Highly configurable as no Driver is needed.
* We can perform multiple operations on the Database records using the APIs.

## Prerequisites ##
The following prerequisites are necessary to implement a connection to your account from Boomi Integration

	*	Set the appropriate Java environment variables on your system, such as JAVA_HOME, CLASSPATH.
	*	To use the connector and implement a connection to your Microsoft Azure Cosmos DB server from 
		Boomi Integration, ensure you have the following:
			*	Hostname or URL for the Microsoft Azure Cosmos DB server.
			*	Deployed Atom on your local machine or host it with Boomi Integration.
			*	Microsoft Azure account for using Microsoft Azure Cosmos DB with the necessary credentials to 
				access Microsoft Azure Cosmos DB APIs and either of the below key tokens.
				Master Key Tokens / Resource Tokens

## Building this connector project ##
To build this project, the following dependencies must be met

 * JDK 1.8 or above
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

The Microsoft Azure Cosmos DB operations define how to interact with your Microsoft Azure Cosmos DB server in Microsoft Azure and represent a specific action (Get, Update, Delete, Create, Upsert, and Query).
Create a separate operation component for each action/object combination that your integration requires.
The Microsoft Azure Cosmos DB connector supports the following operations:

*	Inbound: Get, Query.
*	Outbound: Update, Delete, Create, Upsert.

### Operation Details ###

### Get ###
Get is an inbound action for which you provide the details of the record which you want to retrieve from the Server.
### Update ###
The update is an outbound action where you can update the existing records in the Microsoft Azure Cosmos DB 
database either in bulk or a single record by specifying Object/Record ID(s) and Partition Key. If the ID 
exists, the connector automatically overrides the existing record in database and performs an update operation
and If the ID does not exist, the connector will not process the record.
### Delete ###
Delete is an outbound action where you can delete the existing records in the Microsoft Azure Cosmos DB database either in bulk or a single record by specifying Object/Record ID(s) and Partition Key. If ID and Partition Key exists, the connector will automatically delete the existing record and If ID and Partition Key do not exist, the connector will ignore the record.
### Create ###
Create is an outbound action where you can Upload and add a new document to the Microsoft Azure Cosmos DB by specifying Record ID and Partition Key.
### Upsert ###
Upsert is an outbound action to either update or create a new record in Microsoft Azure Cosmos DB collection. This operation can be performed either in bulk or a single record by specifying Object/Record ID(s) and Partition Key. If ID already exists, the connector automatically performs an update operation and If the ID does not exist, the connector will create a new record.
### Query ###
The query is an inbound action that looks up objects in Microsoft Azure Cosmos DB collection and returns zero to many object records. After you select the Query action and use the Import Wizard, the operation component page contains configuration options to add filters and set parameters to limit the results. You can refine the query on the Filters tab and define expressions to create required query logic.

## UserGuide ##
https://help.boomi.com/bundle/connectors/page/int-Microsoft_Azure_Cosmos_DB_connector.html

## Additional resources ##
https://docs.microsoft.com/en-us/azure/cosmos-db/