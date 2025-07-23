## Salesforce REST connector ##

## Prerequisites ##

## Building this connector project ##

To build this project, the following dependencies must be met

* JDK 1.8 or above
* Apache Maven 3.5.0

### Compiling ###

Once your JAVA_HOME points to your installation of JDK 1.8 (or above) and JAVA_HOME/bin and Apache maven are in your
PATH, issue the following command to build the jar file:

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

It will run the unit tests and build the jar file. Both the CAR file, and the Test Reports will be available inside *
target* folder.

## Salesforce REST connection ##

Supports both Basic and OAuth 2.0 authentication. Basic Authentication is done by requesting sessionID from Salesforce
SOAP API, storing the sessionID in connector Cache

## Salesforce REST operations ##

Most operations supports 3 Salesforce APIs

1- Bulk API v2.0: if Operation API field is set to Bulk API v2.0

2- REST API: if Operation API field is set to REST API **and** the batch count is set to **1**

3- Composite API: if Operation API field is set to REST API **and** the batch count is set to **greater than 1**


## Additional resources ##

https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_what_is_rest_api.htm