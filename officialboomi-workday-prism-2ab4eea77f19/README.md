## Workday Prism Analytics V2 connector ##

The Workday Prism Analytics V2 (Tech Preview) connector allows Workday Customers to bring together data from any source, in order to prepare, analyze, and securely share it within your organization. Enabling business insights needed, to drive better business decisions and outcomes. With the ability to bring in external data sources into Workday Prism Analytics and govern them along with existing Workday Prism Analytics data, the business will be enabled to generate financial and HR insights that can be shared across your organization. Through this connector, you can use a Dell Boomi Integration process to perform operations such as GET, CREATE (Dataset/Table and Bucket), UPLOAD, COMPLETE BUCKET, and IMPORT.

 ### Prerequisites ###

To use the connector and implement a connection to Workday Prism Analytics from Boomi Integration, have the following information and access rights:

*  An Atom deployed on your local machine or hosted with Boomi Integration.
* Set the appropriate Java environment variables on your system, such as JAVA_HOME, CLASSPATH.
* Configure below actions for Workday Prism Analytics connector in the Prism Analytics platform.
     * API Service Endpoint: The URL to your Workday REST API service endpoint. You can find this endpoint in Workday using the View API Client task.
     * Workday Client ID and Secret: The client ID and secret are generated for you in Workday when you register the API client in your tenant using the Register API Client for Integrations task.
	 * Refresh Token: You can generate a refresh token in Workday when you register the API client in your tenant using the Register API Client for Integrations task.
* Have the appropriate access rights and permissions to create Workday Prism Analytics datasets & tables using the Workday user interface. The API uploads are performed with this user.
* Generate Access Key and Secret Key for the which is used in our connector to communicate to Workday services.
* Dell Boomi Username and password with the necessary credentials.

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

The Workday Prism Analytics V2 (Tech Preview) operation defines how to interact with your Workday account and represents a specific action to be performed (Get, Create (Dataset/Table and Bucket), Upload, Complete Bucket, and Import.
Create a separate operation component for each action/object combination that your integration requires.
The workday Prism Analytics V2 (Tech Preview) operations support the following actions:

*	Inbound: Get.
*	Outbound: Create (Table and Bucket), Upload, Complete, and Import.

### Get ###
Use the Get operation to obtain information about a bucket e.g. Bucket name, the associated dataset, and the status of the bucket by specifying the Bucket ID as an input parameter in the configuration of the connector shape.

### Create ###
Create is an outbound action with which you can do the following. 

* Create a table.
* Create a bucket.

#### Creating a table ####
In order to load data into Workday Prism Analytics, the first step is to use the Create action to create a new empty table.

#### Creating a bucket ####
After creating the dataset, your next step is to use the Create action to create the bucket, which is a temporary folder for the CSV files that you want to upload to the dataset. 

When browsing to create a bucket, you can select the Use existing schema option to retrieve the schema fields from the dataset that has already been uploaded and use the schema in the new bucket you are creating. If the dataset does not have an uploaded schema, or you do not select this option.  You must define the schema in an input document.

### Upload ###
The upload operation is used to upload the CSV files into the bucket after it is created.

### Complete ###
The Complete Bucket operation is used to initiate the data transfer from bucket to dataset once the CSV files are uploaded successfully into the bucket.

### Import ###
This operation allows the users to create a bucket and upload the files in the dataset for a preexisting Table. Upon execution, this operation will execute the following
* Creates a bucket for an existing table. 
* Uploads the file to the bucket.
* Completes the bucket

## UserGuide ##
Workday Prism Analytics V2 (Tech Preview) connector: https://help.boomi.com/bundle/connectors/page/int-Workday_Prism_Analytics_V2_connector.html

## Additional resources ##
Workday REST API (https://doc.workday.com/reader/wsiU0cnNjCc_k7shLNxLEA/HvgwLwxCHVdBlZUTNd9s7A).
Configuring OAuth 2.0 for Your REST API Client (https://doc.workday.com/reader/wsiU0cnNjCc_k7shLNxLEA/JfMMxf0x4NmfBKVT~6vgbw).

Note: You must log in to the Workday Community to access these resources.