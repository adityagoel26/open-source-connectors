# SAP JCo Connector #
The SAP JCo connector (classified as an Enterprise connector) enables you to connect to your SAP NetWeaver-based application and get or send data using BAPIs, Remote Function Modules (RFMs) and asynchronously using IDocs. 
You can browse the list of available BAPI/RFMs and IDocs available in your SAP system and automatically generate the request and response profiles to use in Boomi Integration processes and maps.
The SAP JCo connector supports the following connectivity to SAP systems using the SAP Java Connector library:
* Discovery of BAPIs using the Business Object Repository by specifying a specific business object, or a wildcard containing a partial object name.
* Query of a specific BAPI/RFM enabled function module by specifying the BAPI or Remote Function Module name.
* Sending IDocs to, and receiving IDocs from SAP by specifying the IDoc via Base Type, Extension and Segment or Application Release as option to build the segment profile.

## Connector Configuration ##

To configure the connector to communicate with SAP, please set up two components:
* SAP JCo connection
* SAP JCo operation

This design provides reusable components containing connection settings such as user name, password, and SAP-specific settings including IDoc TID support. After building your connection and connector operation, set up your connector operation within a process. When the SAP connector operation is defined properly within your process, Boomi Integration can map to and from virtually any system using the connector to retrieve data from or send data to SAP.
## Prerequisites ##
To use the connector and implement a connection to SAP Server from Boomi Integration, do the following:
* Set the appropriate Java environment variables on your system, such as JAVA_HOME, CLASSPATH.
* Obtain the following SAP Java Connector library files:
* SAP Java Connector Release 3.0.x
* SAP Java IDoc Class Library Release 3.0.x
* Deploy the Atom to a server that has access to the SAP system for the ports used by SAP for communication (i.e., the same LAN or access using SAProuter) to ports required to support RFC/BAPI and IDocs.
* Set up an Atom to reside in the same network as the SAP system (Boomi recommends).
* Verify that the SAP endpoints such as RFC, BAPI, or IDoc exist in SAP.
* Configure Boomi Integration with an SAP user name with sufficient permissions to perform the tasks necessary for the integration (such as being able to invoke RFCs and BAPIs, manipulate specific business objects, etc.).

## Additional Atom configuration ##
The SAP JCo connector requires SAP Java Connector libraries to be loaded into the Atom.
1.	Download the SAP Java Connector files from the SAP Support Portal (either SAP JCO 3.0 or 3.1) (https://support.sap.com/en/product/connectors/jco.html).
Note: Download the SAP JCo ZIP file associated with your OS/Hardware combination which applies to your Atom. Refer to your SAP documentation or contact your SAP administrator to gain access to the SAP Support Portal with a valid S-user ID with the appropriate authorization to download the ZIP file.
2.	Copy the files into the Atom installation.
a.	In your Boomi Account, go to Setup -> Account Libraries
b.	Upload the SAP Jco jars sapjco3.jar and if using SAP IDoc, then sapidoc3.jar
c.	To Receive IDocs from the SAP system in Listen or Send operations, the SAP connector requires access to a database which it will create a table to track the TIDs that the connector receives and processes. To enable TID management, make sure the Atom can access a database that provides a JDBC driver. Upload JDBC or other driver jar files in Account Libraries.
d.	Go to Build and create a new Custom Library
e.	Select the Custom Library Type -> Connector, and select SAP JCO as the connector type
f.	Add the Jar files which you have uploaded in above points B and C to this Custom Library and Save/Close.
g.	Go to Deploy, select Custom Libraries (dropdown from Process), and you’ll see the Custom Library which you had created
h.	Deploy to the Environment which your atom is attached to
i.	Move the Operating system specific file such as .dll or so files directly into userlib/sapjco subdirectory.  For example:
* Windows: sapjco3.dll
* Linux: libsapjco3.so
j.	Restart the Atom.

## Additional SAP and database setup to receive IDocs ##
Note: Your SAP Administrator/BASIS professional should provide Program ID information which is used for Boomi to connect to SAP to receive IDocs.
The SAP connector automatically creates a table for Transaction ID Management in a database (if they do not already exist) during the initial deployment of a process to receive IDocs. For reference purposes, additional details about the TID database is provided if your database administrator is required to create the table
Important: The TID database size grows over time and you are responsible for managing it, including purging the database when necessary. Only delete those records with a status of Confirmed (onConfirmTid). See the Transactional RFC Server Programs article from the SAP documentation for more information.
### Table Name ###
### TID_MGMT ###
### Columns ###
### TID varchar (255) ###
### PROG_ID varchar (255) ###
### STATUS varchar (255) ###
### Primary Key ###
### TID, PROG_ID ###
### STATUS Values ###
CHECKED - initial receipt of IDoc message. 
CONFIRMED - all RFC functions have successfully been completed.
COMMITTED - all RFC functions have successfully been handled by the SAP connector.
ROLLBACK (?)
## IDoc Listener on a Molecule ##
The listener process is deployed to and runs on every node in a Molecule, and each node server name is identified by a generated unique ID (UUID). All nodes in the Molecule that are connected to the SAP server using the SAP connector listen for IDocs via the specific Program ID associated the Registered Server Program for the RFC Destination in SAP SAP handles the distribution of the IDocs across the RFC destination, therefore, Boomi does not have any control over which Boomi molecule node will handle the specific request. If a node fails and becomes offline, the SAP server automatically sends the IDoc to another available node in the Molecule for processing.
## Supported editions ##
The following editions are supported:

* All SAP Business Suite solutions which are accessible using SAP Java Connector 3.0.x and 3.1
* SAP R/3 4.0, 4.5, 4.6, 4.7
* SAP ERP 6.0
* SAP Solutions based on SAP Netweaver Application Server 6.10, 6.20, 6.30, 2004, 7.0, 7.51
* SAP S/4HANA EX (STE/On-Premise)

## Tracked properties ##
This connector has the following tracked properties that can be set or referenced in various shape parameters.
Name (Connection Operation)
Description 
### Transaction Id (Send) ###
The transaction Id of the process.
### Status (Listen) ###
Indicates the status of IDoc
### Idoc Type (Listen) ###
Indicates the type (specific format and structure) of the IDoc.
### Receiver Port (Listen) ###
Receiver port (SAP System, EDI subsystem)
### Partner No of Receiver (Listen) ###
Indicates the Partner Number of Receiver
### Test Flag (Listen) ###
Indicates the Test Flag of the operation.
### Sender Port (Listen) ###
Sender port (SAP System, EDI subsystem)
### Partner Type (Listen) ###
Partner type of sender
### Partner Number (Listen) ###
Partner Number of Sender
### Created Date (Listen) ###
IDoc Created On
### Created Time Listen) ###
IDoc Created at
### Additional resources ###
How to Configure SAP R/3 for IDoc and BAPI/RFM Connectivity

## SAP JCo Connection ##
The SAP JCo connection represents a single account including login credentials. If you have multiple systems, you need a separate connection for each.

### Connection tab ###
Name
Description
### Connection Type (Mandatory Field) ###
Select one of the available SAP connection types to use to connect to SAP:
* Application Server Host — (ASHOST) is a host-based connection.
* Message Server Host — (MSHOST) is a load-balanced connection where you specify an optional server group to use for load balancing.

### Server (Mandatory Field) ###
Enter the hostname or IP address of the SAP server. (include the SAProuter string if applicable). The SAP JCo property is jco.client.ashost. if you set Connection Type to Application Server Host. When you set Connection Type to Message Server Host, the SAP JCo property is jco.client.mshost. The option you do not select will have an empty string for its corresponding JCo property.
### User Name (Mandatory Field) ###
Enter the SAP Username used to connect to an SAP system. The SAP JCo property is jco.client.user.
### Password (Mandatory Field) ###
Enter the SAP Password used to connect to an SAP system. The SAP JCo property is jco.client.passwd.
### Client (Mandatory Field) ###
Enter the SAP client number. The SAP JCo property is jco.client.client.
### Language Code (Mandatory Field) ###
Use the drop-down to select one of the language codes saved with the connection component. The language codes are EN (English), DE (German), IT (Italian), and JA (Japanese).
### Custom Language Code ###
If the desired language code is unavailable in the above drop-down, enter the desired language code into the field.  If there is a language code unavailable in the drop-down, select the Enter button and enter the desired language code into the field. The SAP JCo property is jco.client.lang.
### System Number (Mandatory Field) ###
If the Connection Type is Application Server Host, enter the SAP system number (SYSNR). If the Connection Type is Message Server Host, value is set to an empty string. The SAP JCo property is jco.client.sysnr. If you set the Connection Type to Message Server Host, the JCo property value is set to an empty string. This field is extensible.
### SAP System Name ###
If the Connection Type is Application Server Host, value is set to an empty string. If the Connection Type is Message Server Host, enter the name of the SAP back-end system ID (SID). The SAP JCo property is jco.client.r3name. If you set Connection Type to Application Server Host, the JCo property value is set to an empty string. This field is extensible.
### SAP Login Group Name ###
This field is applicable only if the Connection Type is Message Server Host, enter the name of the SAP server group (group).  The SAP JCo property is jco.client.group. This field is extensible.
### Enable Trace? ###
This setting controls whether the SAP traces the logs during connector calls:
* If cleared by default, the JCo property value will be null. These JCo property value defaults will not be set by Boomi Integration.
* If selected, the connector creates trace logs to debug SAP connector calls and sets the JCo property value and its property value is 1.
The SAP JCo property is jco.client.trace. For more information about SAP trace activation, go to SAP JCo (Java Connector) and tracing.

### SAP Maximum Idle Connections ###
Sets the maximum number of idle connections to keep open by the destination. A value of 0 creates no connection pooling and connections are closed after each request.  The SAP property is jco.destination.pool_capacity. There is no default number in Boomi Integration.
### SAP Maximum Active Connections ###
Sets the maximum number of active connections that can be created simultaneously for a destination.  The SAP property is jco.destination.peak_limit. There is no default number in Boomi Integration.
### SAP Idle Time ###
Sets the interval in milliseconds (ms) after which connections held by the destination can be closed. The SAP property is jco.destination.expiration_time. There is no default number in Boomi Integration.
### The following fields are applicable if you are planning to use the LISTEN operation: ###
### Listener Gateway Host (Mandatory Field) ###
Enter the Gateway Hostname (GWHOST name) of the SAP server. Include the SAProuter string if applicable. The SAP JCo property is jco.server.gwhost.
### Listener Gateway Service (Mandatory Field) ###
Enter the Gateway port name or number of the SAP server.  Usually, the entry is sapgwXX or 33XX, where XX is the system number defined in the Settings tab. The SAP JCo property is jco.server.gwserv.
### Listener Connection Count ###
The number of server connections for Listen operation per node.  By default, there are two listener connections per atom or molecule node.  One listener per node is the minimum. The SAP JCo property is jco.server.connection_count.
### Additional Connection Settings (Optional) ###
Specify a properties file to configure additional JCo properties to be used by the connection.
### Database User Name ###
Enter the database username.
### Database Password ### 
Enter the database user password.
### Database Driver Type (Mandatory Field) ###
Select the Driver Type used to connect to the database from the dropdown. Below are the options provided in the dropdown.
* SQL Server (jTDS).
* Oracle.
* MySQL.
* SQL Server (Microsoft).
* SAP Hana.
* Custom.
Select the type of driver to connect to from the drop-down list or specify the Connection URL directly by selecting Custom and adding the driver(s) to the Atom.
Upload the JAR files into your Boomi Integration account library (Setup > Account Libraries), add those files to a Custom Library component, and deploy the component to the appropriate Atom, Molecule, Atom Cloud, or environment.
Note: Uploaded or imported files are first passed through a virus scanner. The upload or import results in an error if a virus is detected, and the file is rejected. Please contact Boomi Support if an error persists.

### Custom Driver Class Name ###
Enter the qualified Java class name of the JDBC driver, which you can get from the JDBC vendor's documentation, for example, sun.jdbc.odbc.JdbcOdbcDriver
Note: This field is mandatory only if the Database Driver Type is selected as Custom.
### Custom Connection URL ###
Refer to the JDBC vendor's documentation for the connection URL syntax. The syntax may have a pattern and URL as below
* Example pattern: jdbc:<database type>://<host>:<port>/<database name>;<additional options name/value pairs>
* Example URL: jdbc:mysql//localhost:3306/MyDatabase;option1=value;option2=value
Note: This field is mandatory only if the Database Driver Type is selected as Custom.

### Database Host (Mandatory Field) ###
Enter the name or IP Address of the database server. 
### Database Port ###
Enter the Port number used to connect to the database server. Some common defaults are:
* SQL Server: 1433 
* Oracle: 1521 
* Sybase: 5000 
* Derby: 1527 
* DB2: 50000 
* MySQL: 3306 

### Database Name (Mandatory Field) ###
Enter the name of your database.
### Database Additional Options ###
Enter any additional configurable options to be specified in the database URL. These options are appended to the end of the connection URL according to your database vendor. The options can be name/value pairs delimited by semicolons, such as:
* ;instance=DB01
* ;domain=<Your Windows Domain Name> (when connecting to SQL Server as a Windows user)
Note: When using SQL Server, connecting as a Windows user, and connecting to a named instance, the order of the properties for the JDBC URL is important. For example: instance=<value>;domain=<value>.

### Minimum Connections ###
Minimum database connections required for connection pooling 
### Maximum Connections ###
Maximum database connections required for connection pooling (-1 for unlimited).
### SAP JCo properties ###
The following table matches fields in the Settings tab, Listener Settings tab, and in an SAP listener operation with their corresponding JCo property. The table also notes if a JCo property only applies to an Application Server Host (ASHOST) or Message Server Host (MSHOST).
Field	SAP JCo property
Connection Type: Server (ASHOST)	jco.client.ashost
Connection Type: Server (MSHOST)	jco.client.mshost
User Name	jco.client.user
Password	jco.client.passwd
Client	jco.client.client
Language Code	jco.client.lang
System Number	jco.client.sysnr
SAP System Name (For MSHOST)	jco.client.r3name
SAP Login Group Name (For MSHOST)	jco.client.group
Enable Trace?	jco.client.trace
SAP Maximum Idle Connections	jco.destination.pool_capacity
SAP Maximum Active Connections	jco.destination.peak_limit
SAP Idle Time	jco.destination.expiration_time
Listener Gateway Host	jco.server.gwhost
Listener Gateway Service	jco.server.gwserv
IDoc Name (SAP Listen operation)	jco.server.repository_destination
Program ID (SAP Listen operation)	jco.server.repository_destination 
Listener Connection Count	jco.server.connection_count

Note: All JCo properties are supported, however, the behavior of each property is dependent upon SAP.
### Properties files ###
The JCo properties you specify in a properties file depend on your use case. A properties file can contain as many properties as needed. The properties are recognized as long as they are valid and located in the file name entered in Additional Connection Settings. It is best practice to save your properties file in the following format: <unique_name>_jco.properties.
The following example shows the contents of a properties file and how it should be formatted:
jco.client.ashost=/H/#.###.###.#/H/###.##.##.##/H/xxx####
jco.client.user=username
jco.client.passwd=password
jco.client.client=###
jco.client.lang=EN
jco.client.sysnr=#
jco.client.trace=1
jco.server.gwhost=/H/#.###.###.#/H/###.##.##.##/H/xxx####
jco.server.gwserv=sapgw##

## SAP JCo Operation ##
The SAP JCo operation defines how to interact with the SAP Application and represent a specific action (Send, Execute and Listen) to be performed against a specific file.

Use the Import Wizard to browse the BAPIs available in your system's Business Object Repository by a specific object or by specifying the specific RFM or BAPI to use. The wizard automatically generates the request and response XML profiles for the function or IDoc, representing the input and output parameters for the function call. Create a separate operation for each RFM, BAPI or IDoc call required for your integration.

### Options tab ###
Select a connector action and then use the Import Wizard to select the object with which you want to integrate. When you configure an action, the following fields appear on the Options tab.
Name
Description
### Connector Action ###
Determines whether the action is EXECUTE for BAPIs/RFMs, SEND for sending IDocs to SAP or LISTEN for Boomi as a IDoc Listener to SAP.
###Object (Execute, Send and Listen) ###
Defines the object with which you want to integrate, which was selected in the Import Wizard.
### Request Profile (Execute and Send) ###
The XML profile definition that represents the structure that is being sent or received by the connector.
### Response Profile (Execute and Listen) ###
The XML profile definition that represents the structure that is being sent or received by the connector.
### Tracking Direction (Execute, Send and Listed) ###
This read-only setting shows you the tracking direction (either Input Documents or Output Documents) for the current operation. 
This setting is determined by the operation configuration in the connector descriptor and affects which document appears in Process Reporting. See the @trackedDocument attribute in the Connector descriptor files topic for more information.
Return Application Error Responses (Execute, Send and Listed)
This setting controls whether a server error prevents an operation from completing:
* If cleared, the process aborts and reports the error on the Process Reporting page.
* If selected, failed operations will not be reported in Manage, allowing you to act on them in your process.

### Commit Transaction? (Execute, Send and Listed) ###
Select if the specific BAPI requires a "Commit Transaction" as part of the BAPI call. See the BOR documentation in SAP for more information.
### Program ID (Listen) ###
Program ID as set up in Database. This ID can also be entered in the Program ID field in the Import Wizard.

## Execute ##
Execute a BAPI or RFM where the connector makes a request and the SAP application responds with the data from the selected SAP application.
In order to generate the request and response XML profile associated with the BAPI or RFM, you must use the Import Wizard.
Below Steps are to be followed during Import using JCo Import Wizard.
1.	Click the Import button in the Operation window.
2.	Choose the Atom and the Connection component that connects to the SAP JCo server instance. An Atom must already be deployed either in Local System must meet the SAP JCO library prerequisites.
3.	Select Function Type as Business Object or BAPI/RFM.
•	If Business Object is selected, enter the value of the Business Object in the Function Filter field. This is the Object Name as defined in the Business Object Repository (BOR). If a wildcard is entered in the Function Filter field, all the released BAPIs for the associated object is retrieved for preview and available for selection as Object Type.
•	If BAPI/RFM is selected, enter the exact name of the BAPI or RFM in the Function Filter Field. There will be no option listed to select as Object Type except the name of the BAPI or RFM is provided.
4.	Click Next, Review the Object Type selected, Request and Response Profiles created and click Finish. 
Once the import of the XML profiles and the setup of the connector operation has completed, the imported Request and Response XML profiles are populated in the Options tab.
5.	Review the BOR documentation in SAP associated with the specific BAPI to see if it requires a "Commit Transaction" as part of the BAPI call. If it is required, turn on the Commit Transaction? checkbox on the Options tab.

## Send ##
Send is an outbound action that sends IDocs to the destination SAP Application
Below Steps are to be followed during Import using JCo Import Wizard.
1.	Click the Import button in the Operation window.
2.	Choose the Atom and the Connection component that connects to the SAP JCo server instance. An Atom must already be deployed either in Local System or in Cloud.
3.	Select Function Type as IDoc.
4.	Specify the IDoc Base Type to be imported into the connector operation. If Valid, it will be displayed as Object Type which can be selected. If not Valid, the Import wizard will throw an error meaning you cannot complete import Option.
5.	Optional: Specify the IDoc Extension, Segment Release and Application Release to be imported into the connector operation.
6.	Click Next, Review the Object Type selected, Request and Response Profiles created and click Finish.
Once the import of the XML profiles and the setup of the connector operation has completed, the imported Request XML profile will be populated in the Options tab.

## Listen ##
Listen operation is used to receive IDocs from the destination database in SAP server as on when a new IDoc is added. The Listen operation must be the first step in your process. This process should be configured using START shape as a connector (with connection and operation properties). Once the process is set up, the complete process should be deployed in the Atom used (local or cloud) in the Dell Boomi platform. The added process can be managed in the “Atom Management” section under the Manage tab at the top.

### Below Steps are to be followed during Import using JCo Import Wizard. ###
1.	Click the Import button in the Operation window.
2.	Choose the Atom and the Connection component that connects to the SAP JCo server instance. An Atom must already be deployed either in Local System or in Cloud.
3.	Select Function Type as IDoc.
4.	Specify the IDoc Base Type to be imported into the connector operation. If Valid, it will be displayed as Object Type which can be selected. If not Valid, the Import wizard will throw an error meaning you cannot complete import Option.
5.	Optional: Specify the IDoc Extension, Segment Release and Application Release to be imported into the connector operation 
6.	Specify Program ID as set up in the SAP application. This ID can also be entered directly in the Program ID field in the Options Tab post successful import.
7.	Click Next, Review the Object Type selected, Request and Response Profiles created and click Finish.
Once the import of the XML profiles and the setup of the connector operation has completed, the imported Response XML profile will be populated in the Options tab.
8.	If not specified in Import Wizard, specify the Program Id as set up in SAP in transaction SM59 as the Registered Server Program in the Options tab

### Archiving tab ###
See the Connector operation’s Archiving tab for more information.

### Tracking tab ###
See the Connector operation’s Tracking tab for more information.

## Specifying SAP JCo properties with a properties file ##

You can specify SAP JCo properties using the Additional Connection Settings field. This feature requires a properties file containing additional JCo properties you want to specify. Values in the Settings tab can be used with properties set in the file, but values set in the file take precedence.

## About this task ##
The SAP connector will recognize additional JCo properties or JCo properties normally set by the UI. To specify these JCo properties with a properties file, you must create the file and then upload it to your Atom in a JAR file.

Note: This process uses Custom Library components. Custom libraries are not supported for listeners in an Atom Cloud. If you need to upload properties files for use in a listen operation on a private Atom Cloud, complete steps 1-3 and contact your Cloud owner. They need to manually place your JAR files in the Atom's [AtomRoot]/userlib folder. When the files are added, complete steps 10-12.
1.	Create a local folder on your system.
2.	Create a properties file.
a.	Open a text editor such as Notepad. 
b.	Enter the JCo properties you want to specify and each corresponding value. For example, you might enter jco.client.user=BOOMI3. 
c.	Save the properties file as <unique_name>_jco.properties.
Note: JARs can contain multiple properties files, so you should create unique names for every properties file. For example, a properties file could have the following name: SAPatom2_jco.properties.
3.	Use the JAR utility that is provided by the Java SDK (jar- The Java Archive Tool), and create a JAR file containing the properties file. JAR files can contain multiple properties files, however, do not place the same files in multiple JARs.
Creating a JAR file tutorial from the Java Tutorials will help you when creating a JAR file.
a.	Use the command line to navigate to the directory containing your properties file. 
b.	Create the JAR file using the basic format of the command for creating a JAR file: jar cf jar-file input-file(s). You can add additional options to the cf options of the command. For example: jar cvf <JarFileName.jar> <FilesToBeCompressed>. Your command may look similar to the following: jar cf SAPJCoProps.jar *.PROPERTIES.
This will compress your properties file into a .jar file. For example SAPJCoProps.jar.
4.	Upload the custom JAR file to an account by using the Manage Account Libraries page (Setup > Account Libraries).
To upload files, you must have the Build Read and Write Access privilege and the Developer privilege. Typically, an administrator uploads the files for an account. Each Boomi account has access to 100 MB of space for uploaded files.
Note: Uploaded or imported files are first passed through a virus scanner. The upload or import results in an error if a virus is detected, and the file is rejected. Please contact Boomi Support if an error persists.
5.	Create a Custom Library component. Select General in the Custom Library Type drop-down list, and add the JAR file to the Custom Library component.
6.	Click View Deployments.
7.	Attach the environments that you want to deploy to. Do not deploy multiple files with the same name to a single environment.
8.	Deploy the Custom Library Component. The JAR file is created under the [AtomRoot]/userlib folder. The connector can load the parameters from the configuration files in the JAR as a configuration resource.
9.	Restart the Atom. 
Note: If you use a Molecule and you have enabled forked execution for the Molecule, you do not have to restart the Molecule.
10.	On the Build page, create or open the SAP connection.
11.	Enter the name of the properties file in Additional Connection Settings. The format must match exactly.
12.	Save the connection.
