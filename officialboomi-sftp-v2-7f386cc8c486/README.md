## SFTP V2 (Tech Preview) connector ##

The SFTP V2 (Tech Preview) connector allows you to transfer data from and to an SFTP server using Java Secure Channel (JSch). Unlike the standard FTP connector, the SFTP V2 (Tech Preview) connector encrypts both data and commands to ensure that no sensitive data is exposed when transmitted over a network. Using this connector, you can use a Boomi Integration process to GET, CREATE, QUERY, LIST, UPSERT, and DELETE the data in any format.

## Prerequisites ##

The following prerequisites are necessary to implement a connection to your SFTP server from Boomi Integration

*  An Atom deployed on your local machine or hosted with Boomi Integration.
* Set the appropriate Java environment variables on your system, such as JAVA_HOME, CLASSPATH.
* Configure the below as mentioned, for SFTP V2 (Tech Preview) connector to communicate with SFTP:  
	 * SFTP users have the necessary authorization to access remote directories.
     * Hostname and port number of the SFTP server.
	 * One of the following authentication types:
	       * Username and Password.
		   * Using Public Key (the Client SSH Key File Path or Public and Private Key Content is needed).
* Dell Boomi Username and password with the necessary credentials.

## Building this connector project ##
To build this project, the following dependencies must be met

 * JDK 1.8 or above
### Compiling ###
Once your JAVA_HOME points to your installation of JDK 1.8 (or above) and JAVA_HOME/bin is in your PATH, issue the following command to build the jar file:
```
  ./gradlew build
```
or if you don't want to run the unit tests, then run 
```
  ./gradlew build -x test
``` 
### Running the unit tests ###
To run the unit tests, please use below command 
``` 
  ./gradlew test 
```
It will run the unit tests and build the jar file. Both the CAR file and the Test Reports will be available inside build folder.

## Supported Operations ##

The SFTP V2 (Tech Preview) operations define how to interact with your SFTP server and represent a specific action to be performed against a specific file.

Create a separate operation component for each action/object combination that your integration requires.

The SFTP operations support the following actions:

*	Inbound: Get, List, Query.
*	Outbound: Create, Upsert, Delete.

### Get ###

Get is an inbound action for which you provide the details of the record you want to retrieve from the SFTP Server. When using Get to download a filefrom the server, the file name must be specified as the parameter.

### Create ###

Create is an outbound action that you can use to upload and add a new file to the server. The request profile contains the properties for a single object. Upon successful record creation, the operation returns a response containing the fully populated record. The file name must be provided using a File Name document property. 

### Query ###

Query is an inbound action that looks up objects (Files) in the SFTP server and returns zero to many object records from a single Query request based on the filters applied. 
  
####Filters####

*    Filter Name*: Specify a name for the filter expression.
*    Field*: File search can be performed based on fileName, fileSize, & modifiedDate.
*    Operator*: Based on the search criteria value, operator options will vary as listed below.
*    fileName: Regex match, Wildcard Match, Equals, Does not Equal, Less Than, Greater Than
*    fileSize: Equals, Does not Equal, Less Than, Greater Than
*    modifiedDate: Equals, Does not Equal, Less Than, Greater Than

### List ###

List is an inbound action similar to the Query operation which looks up objects (Files) in the SFTP server. However, the List operation will only list the files (not file content) based on the filters. The filters mentioned in the Query operation also applies to List operation.

### Upsert ###

Upsert is an outbound action to either upload and create a new file in the specified remote directory or to perform an update to a file if it exists in the SFTP server. This operation can be performed either as bulk or single by specifying file name and location of the file in a remote directory.

*  If file name already exists, the connector automatically overrides the existing file in the SFTP server and replaces it with the new file.
*  If the file name does not exist, the connector will create a new file in the SFTP server.

### Delete ###

Delete is an outbound action which deletes a file from a remote directory in the SFTP Server by providing the file name and file directory. The file name must be given in the request profile as shown below. We can provide multiple file names within a Request profile.

<DeleteProfileConfig>
 <id>SFTP.txt</id>
</DeleteProfileConfig>

### Listen ###

Listen operation can be used to listen to the events in the root directory of the remote SFTP server. It listens to events like file creation, file deletion or any modification on the existing file in the SFTP server remote directory. The Listen operation also operates in the Singleton mode.
To start listeners in singleton mode, you must manually set the com.boomi.container.bounded.listen.enabled container property value to true. When set to true, the Atom invokes the isSingleton method of the connector operation and singleton mode is activated if the method returns true. This operation does not have a request profile


## UserGuide ##

SFTP V2 (Tech Preview) connector: https://help.boomi.com/bundle/connectors/page/int-SFTP_connector.html

## Additional resources ##

http://www.jcraft.com/jsch/