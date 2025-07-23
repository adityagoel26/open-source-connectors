## OpenManage Enterprise connector ##
OpenManage Enterprise connector allows you to connect to DELL OpenManage Enterprise (OME) and use a process to query Alerts (based on ID, SeverityType and Timestamp filters) in DELL OME and receive output data in JSON format.

The following are some benefits of using the new DELL OME connector.
*	Ability to access and share data from the DELL OpenManage Enterprise Systems.
*	Ability to selectively choose alerts based on filters (ID, SeverityType and Timestamp).

## Prerequisites ##
The following prerequisites are necessary to implement a connection to your account from Boomi Integration

	* Set the appropriate Java environment variables on your system, such as JAVA_HOME, CLASSPATH.
	* IP Address and Port Number of OME Server for the integration.
	* UserName and Passowrd for authenticating into the server.

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
The OME operations define how to interact with DELL OME and represent a specific action (Query) to be performed against a specific file.
Create a separate operation component for each action/object combination that your integration requires.
The OME operations support the following actions:

* Inbound: Query

### Operation Details ###

## Query ##
The query is an inbound action that looks up data in DELL OME and returns zero to many object records from a single Query request based on filters. After you select the Query action and use the Import Wizard, the operation component page contains configuration options to add filters and set parameters as connection property to limit the results. You can refine the query on the Filters tab and define expressions to create required query logic. 

## UserGuide ##
https://help.boomi.com/bundle/connectors/page/int-OME_connector.html

## Additional resources ##
https://www.dell.com/support/manuals/in/en/indhs1/dell-openmanage-enterprise-v3.1/ome_3.1_ug/about-dell-emc-openmanage-enterprise?guid=guid-72dc4319-05e8-4b62-902b-1b49a7a61488&lang=en-us
