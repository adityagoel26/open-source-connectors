# SAP HANA Cloud Connector #
SAP HANA Cloud connector allows you to transfer single/multiple data in JSON format from/to SAP S/4HANA Cloud by connecting to BAPIs. This Connector will be getting all the metadata about available services from SAP API Business Hub. This connector uses the OData service to connect to SAP BAPIs and OData APIs. For OData, once you get the metadata for building the request/response profiles and based on the operation performed, it supports making the REST call with the appropriate operation. 
Through this connector, you can use a Dell Boomi Integration process to Get, Create, Query, Update, and Delete with Params the data. 

## Prerequisites ##
The following prerequisites are necessary to implement a connection to SAP S/4HANA Cloud from Boomi Integration, do the following:

	* Host and Login credentials details of SAP S/4HANA Cloud (Host, SAP User ID, and SAP Password) to perform the selected operation.
	* API key and Login credentials for SAP API Business Hub (API Key, SAP API Business Hub Username, and SAP Business Hub Password) to get Swagger document to build request and response profiles.

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
	
## Supported editions ##
	* Swagger 2.0 for API specification document.
	* Note: Other API specification formats like Open API 3.0, etc. are not supported currently.

# Supported operation #
The SAP HANA Cloud operation defines how to interact with SAP S/4HANA Cloud and represents a specific action (Create, Get, Delete, Query, and Update) to be performed against a specific file.
Create a separate operation component for each action/object combination that your integration requires.
SAP HANA Cloud operation supports the following actions:
* Inbound: Get and Query
* Outbound: Create, Delete and Update

The SAP Hana Cloud Connector is implemented with a common browser mechanism which is responsible for fetching the Swagger/WSDL document depending on the API’s and creating/building the request and response profiles for the mentioned operations.

## GET ##
Get is an inbound action for which you provide the details of the record which you want to retrieve from SAP S/4HANA Cloud. When using Get to download a record, the Parameter Fields listed in the selected URL during import action should be specified as parameters in connector property. If the parameters are not specified, the connector will throw an error.
Note: The values for the Parameter fields in the selected URL should be specified as parameters.

## Create ##
Create is an outbound action where you can upload and add an entity in SAP S/4HANA cloud by making post API calls of SAP OData. The path parameters present in the create API URL will be added to the request schema. If the Create operation is successful, the entity is created in the database and the connector returns a response in JSON format. The response code for successful operation is 201. To create an entry, you need x-csrf-token in the SAP system. 
## Update ##
The update is an outbound action where you can update the existing entity in SAP S/4HANA cloud by making patch API calls of SAP OData. In Update operation, path parameters present in the update API URL will be added to the request schema. If the Update operation is successful, the entity is updated in the database and the connector returns a response in JSON format. The response code of successful operation is 204 with an empty response body. To update an entry in SAP, you need x-csrf-token and ETag tokens in the SAP system. 
## Delete ##
The Delete is an outbound action where you can delete the existing entity in SAP S/4HANA cloud by making delete API calls of SAP OData. The path parameters present in the delete API URL will be added to the request schema. If the delete operation is successful, the entity is deleted in the database and the connector returns a response in JSON format. The response code of successful operation is 204 with an empty response body. To update an entry in SAP, you need x-csrf-token and ETag tokens in the SAP system. 
## Query ##
The query is an inbound action that looks up objects (File) in SAP S/4HANA Cloud and returns zero to many object records from a single Query request based on filters. After you select the Query action and use the Import Wizard, the operation component page contains configuration options to add filters and set parameters to limit the results. You can refine the query on the Filters tab and define expressions. The response code for successful operation is 200.

## User Guide ##
User guide will be updated soon.
