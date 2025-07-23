# Hubspot CRM Connector

The Hubspot CRM connector will help user to interact with Hubspot account which includes objects that represents types of relationships. User can perform operations like CREATE, ARCHIVE, UPDATE, RETRIEVE and SEARCH. Objects that can be dealt with hubspot crm connector are CONTACTS, COMPANIES, TICKETS and DEALS.In HubSpot, you can show how objects are related to one another by associating their records.Information about records are stored in fields called properties, which are then organized into groups


## Prerequisites

+ Set the appropriate Java environment variables on your system, such as JAVA_HOME, CLASSPATH.
+ Authentication can be done with OAUTH2 or a Private App Access Token
+ For OAUTH2:
  + Find your Client ID and Client secret when you create a new app
  + Provide your required scope
+ For Private App Access Token:
  + Create a token in Settings > Integrations


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


## Connector configuration ##

To configure a Hubspot CRM connector, you must set up two components:

* **Hubspot CRM connection**: used to configure the client ID and client secret needed for authentication.
* **Hubspot CRM operation**: used to configure a `Create`, `Archive`, `Retrieve`, `Search` or `Update` action .


## Supported versions ##

This connector has been tested against Stripe's `/v3/` major API endpoint version.

# Connection #

The Hubspot CRM connection contains all the information that is needed to connect to hubspot account. There are two ways of Authentication type

* **Private App Access Token**
  + Create a token in Settings > Integration



* **OAUTH2**
##### Client ID #####
  + Enter the Client ID used to connect to the account.
##### Client Secret #####
+ Enter the Client Secret used to connect to the account.
##### Scope #####
+ Enter the desired scope used to connect to the account.
##### Access Token #####
+ Generate the access token to connect to the account.


## Request Headers ##
Users have the ability to add custom request headers, which will be utilized across all API calls, ensuring consistent custom header application for every request. To set the header, enter the key and value and select encrypt option 

## Test Connection ##
You can test your connection settings before you use or save the connection in a process. The Test Connection action ensures that your specified connection settings are valid by making a `GET` call to the Hubspot API.


## Supported Operations

The hubspot crm operation define how to interact with your hubspot account database. It supports the following actions:

*	Inbound: Retrieve, Search
*	Outbound: Create, Archive, Update


### Create ###

Create operation enables the creation of new records in HubSpot CRM account for various object types (contacts, companies, deals, tickets). Each create request processes a single record with specified properties and optional associations to other HubSpot objects.The operation validates the input data against the object's schema, ensuring all required properties are provided and properly formatted before creating the record. Requires appropriate HubSpot API scopes for object creation.Returns created record ID and properties in response. The records can be created with and without association. 
* `The structure of create deals with association looks like`
  *'{
  "associations": [
  {
  "types": [
  {
  "associationCategory": "HUBSPOT_DEFINED",
  "associationTypeId": 341
  }
  ],
  "to": {
  "id": 27102659692
  }
  }
  ],
  "properties": {
  "dealname": "Dealcom",
  "pipeline": "default",
  "dealstage": "contractsent"
  }
  }'


### Archive ###

Archive is an outbound action that is used to delete records from the hubspot crm account using unique ID. Returns ID of each object after deletion.
* Performs an HTTP `DELETE` against the endpoint: `/v3/[object_type]/[id]

### Update ###

Update operation allows modification of existing records in HubSpot CRM account by their unique identifier. It supports partial updates of objects where only the specified properties will be modified while maintaining other existing values. The operation can identify objects either by their HubSpot ID or by a unique property like email address. When successful, the operation returns the updated object with its modified properties.
* Properties values can be cleared by passing an empty string

### Retrieve ###

Retrieve operation fetches individual records from HubSpot CRM account using either their unique identifier or email address. It allows retrieval of detailed information for specific objects along with their associated properties and relationships. The operation supports customizable property selection through the output profile, enabling users to specify which fields they want to retrieve. When successful, it returns the complete object data including all requested properties and associations.
* Performs an HTTP `GET` against the endpoint: `/v1/[object_type]/[id]`
* With the connector, user should NOT be allowed to make any selection for properties, properties with history and association and by default data for all the properties and associations (excluding custom objects) are to be returned for the specified ID


### Search ###

Search operation enables querying of HubSpot CRM  records using flexible search criteria and filters. It allows searching across objects based on various property values and conditions. The operation accepts multiple filter criteria and returns matching records up to the specified limit, with results containing all requested properties for each matching object. Search results can be customized through query parameters including sorting options, property selection, and result limits.There are different criteria that are associated with search operation
  * User can select which properties he want to return in output document
  * **Pagination**- Use this to specify the Maximum number of documents to be fetched
      * One record per document
      * Value less than 1 to specify to get all the records, by default set this to -1
  * **Sort**
     * Records can be searched as well as sorted in ascending or descending order
  * **Filter Group**
     * Support AND and OR logic
     * Values in filters are case-insensitive, with the following two exceptions:
        * When filtering for an enumeration property, search is case-sensitive for all filter operators.
        * When filtering for a string property using IN and NOT_IN operators, the searched values must be lowercase
     * Filters are- Less Than, Greater than, Less Than or Equal To, Greater Than or Equal To, Equal To, Not Equal To,Between, HAS Property, NOT HAS Property
     * To include multiple filter criteria, you can group filters within filterGroups
     * 

       



## Documentation

+ https://developers.hubspot.com/docs/api/crm/understanding-the-crm


## Connector Feedback

Feedback can be provided directly to Product Management in our [Product Feedback Forum](https://community.boomi.com/s/ideas) in the boomiverse.  When submitting an idea, please provide the full connector name in the title and a detailed description.

