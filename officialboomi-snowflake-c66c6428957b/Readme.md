## Snowflake connector ##

The Snowflake connector is a key part of the Boomi Integration process that makes it easy to work with Snowflake, one of the fastest growing cloud data management platforms. The Snowflake connector lets users take advantage of all the capabilities a Snowflake data warehouse offers: speed, usability, and flexibility. 
Boomi Integration uses JDBC to connect to the Snowflake Cloud Data Platform. 

## Prerequisites ##

The following prerequisites are necessary to implement a connection to your Snowflake account from Boomi Integration

*  An Atom deployed on your local machine or hosted with Boomi Integration.
* Set the appropriate Java environment variables on your system, such as JAVA_HOME, CLASSPATH.
* Configure the below as mentioned, for Snowflake connector to communicate with Snowflake database:  
     * Snowflake user have the necessary authorization to access database.
     * Connection url of the Snowflake server.
     * One of the following authentication types:
           * Username and Password.
           * Using Private Key and pass phrase.
           * RSA & OpenSSL Key Pair
* Boomi Username and password with the necessary credentials.
* Snowflake requires the following permissions on an S3 bucket and folder to be able to access files in the folder (and sub-folders):
     * s3:GetObject
     * s3:GetObjectVersion
     * s3:ListBucket
     * s3:ListAllBuckets
     
The additional s3:PutObject and s3:DeleteObject permissions are required only if User plan to unload files from the bucket or automatically purge the files after loading them into a table.
Note: This step is optional if User is not using the Amazon S3 buckets.

     
## Building this connector project ##
To build this project, the following dependencies must be met

 * JDK 1.7 or above
 * Apache Maven 3.5.0
 
### Compiling ###
Once your JAVA_HOME points to your installation of JDK 1.8 (or above) and JAVA_HOME/bin and Apache maven are in your PATH, issue the following command to build the jar file:

```
  mvn install -DUserName="username" -DPassword="password" -DAccessKey="accesskey" -DSecretKey="secretkey"
```
where -DUserName represents the Username for the Snowflake DB,
	  -DPassword represents the Password for the Snowflake DB,
	  -DAccessKey represents the AccessKey for the Amazon S3,
	  -DSecretKey represents the SecretKey for the Amazon S3.
or if you don't want to run the unit tests, then run 
```
  mvn install -DskipTests
``` 

### Running the unit tests ###
To run the unit tests, please use below command 

``` 
  mvn install -DUserName="username" -DPassword="password" -DAccessKey="accesskey" -DSecretKey="secretkey"
```
where -DUserName represents the Username for the Snowflake DB,
	  -DPassword represents the Password for the Snowflake DB,
	  -DAccessKey represents the AccessKey for the Amazon S3,
	  -DSecretKey represents the SecretKey for the Amazon S3.
	  
It will run the unit tests and build the jar file. Both the CAR file and the Test Reports will be available inside target folder.

## Snowflake operations ##

A Snowflake operation defines how to interact with the connection, including transactions, batching, custom properties, etc. It supports the following actions:

*	Inbound: Get, Query, Bulk Unload, Bulk Get.
*	Outbound: Create, Update, Delete, SnowSQL, Bulk Load, Bulk Put, Copy Into Location, Copy Into Table .
*  In/Outbound: Execute

### Get ###

Retrieves one or more rows from a database table or view as JSON files to Boomi platform. This operation will fetch the imported database object to Boomi. The Document Batching Option is available in the Browse page. If it is enabled, then the response profile will be generated as an array of JSON object, and the output is batched according to the Batch Size. Otherwise, the response profile is generated as JSON object profile and the output will be one row for one output document. 

### Create ###

Inserts data to Snowflake. Each document input to the connector must contain a single record that will be a row inserted into the target table. This operation has the ability to import generated table profile elements and it supports batching. The Create has both request and response profile. The output to the create operation contains the number of records inserted. The output is displayed based on the Return result option. If it is enabled then the CREATE operation returns the result. Default values for missing fields without batching are supported with a batch size of one. Input options for missing fields can be configured with defaults for empty fields in inputs containing different fields.

### UPDATE ###

Updates the value of data in the database. You use this operation to import generated table profile elements. You can choose the columns which you want to use as the primary key and then give them new values. Works with dynamic data that can be overridden by static data.

### Query ###

Retrieves one or more records from a specific table. By importing a generated profile element, you can see the whole table. It supports sorting and filters. It has the ability to import generated table profile elements. The Document Batching Option is available in the Browse page. If it is enabled, then the response profile will be generated as an array of JSON object, and the output is batched according to the Batch Size. Otherwise, the response profile is generated as JSON object profile and the output will be one row for one output document. Data can be retrieved by applying two or more filters to the same column.Input and output documents can be tracked in Process Reporting, allowing for the direction of changes to be observed.
  
### Delete ###

Deletes record(s) from the database. It uses a message shape to convert the JSON deletion object into XML format, extracting their selection criteria (For example: ID) from the JSON object.

<DeleteProfileConfig>
 <id>{"column name":"column value"}</id>
</DeleteProfileConfig>

### Execute ###

The Snowflake  connector supports executing Snowflake stored procedures. First you must create the stored procedures with the Snowflake DDL command. After this, you can request the response profile for the stored procedure with the Execute action. When calling, using, and getting values back from stored procedures, you might need to do a conversion from a Snowflake SQL data type to a JavaScript data type (or vice versa). 

### SnowSQL ###

The Snowflake  connector supports executing a SQL command.The input can be given either in the SQL Script field or in the SQL Script Document Property.	A JSON document is passed to the connector to SnowSQL operation if needed to send parameters, and this JSON object Should be containing the values of parameters. Parameters� value cannot be a SQL logic. 

### Bulk Load ###

Inserts new data from files on your local disk or Amazon S3 bucket into Snowflake. The Connector loads the file into the Snowflake internal stage or Amazon S3 bucket (external stage) based on the stage details that is provided using PUT command. It is then loaded into the corresponding table from the stage using COPY INTO command. 

### Bulk Unload ###

Retrieves data from a database table or view as CSV/JSON files to Boomi platform. The Connector loads the file into the Snowflake internal stage or Amazon S3 bucket (external stage) based on the stage details that is provided using PUT command. It is then loaded into the corresponding table from the stage using COPY INTO command. 

### Bulk Get ###

Downloads data files from the Snowflake Internal stage to a local directory/folder. Bulk Get doesn't have the Request and Response profile. It retrieves the file from the Internal stage that matches the specified pattern.

### Bulk Put ###

Loads files from our local directory/folder to the Internal stage of Snowflake. Bulk Put doesn't have the Request and Response profile. It retrieves the file from the specified path and loads it into the specified Snowflake internal stage. The file path for the Bulk PUT should be enclosed with single quotes if there is any special characters in the path.

### Copy Into Location ###

Unloads data from the Snowflake table into the files in Internal stage/External stage/External Location. 

### Copy Into Table ###

Loads data from staged files to an existing table. The files must already be staged in Internal stage/External stage/External Location. The Copy Into Table retrieves the file specified in the source location that matches the given pattern and loads the data from the file to the Snowflake table.


## UserGuide ##

Snowflake Release 4 connector: https://help.boomi.com/bundle/connectors/page/int-Snowflake_Connector.html

## Additional resources ##

https://www.snowflake.com/
