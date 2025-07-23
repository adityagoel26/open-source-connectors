# Amazon EventBridge Connector #

Amazon EventBridge connector allows you to transfer any number of events(data) in JSON format to the Event Bus by connecting through Amazon EventBridge. The Amazon EventBridge connector uses AWS Identity and Access Management (AWS IAM) to authenticate users to Amazon. Through this connector, you can use a Dell Boomi Integration process to CREATE the events in JSON format.

The following are the values you recognize by using this connector
* Simplifies application support
* Highly configurable
* Built-in data manipulation
* Real-time support 

Amazon EventBridge is a serverless event bus service that makes it easy to connect your applications with data from a variety of sources.
It simplifies the building and management of event-driven applications by taking care of event ingestion and delivery, security, authorization, and error-handling for you. You can choose an event source on the Amazon EventBridge console and select a target from the list of AWS services and Amazon EventBridge will automatically deliver the events in real time.

To configure the connector to communicate with Amazon EventBridge, please set up two components:

* Amazon EventBridge connection
* Amazon EventBridge operation

This design provides reusable components containing connection settings and operation settings. After building your connection and operation, set up your connector within a process. When the process is defined properly, Boomi Integration can map to and from virtually any system using the Amazon EventBridge connector to communicate with other Amazon services.

The connector supports 
* AWS SDK version 1.11.647  

# Prerequisites #

To implement a connection to your Amazon EventBridge account from Boomi Integration and to send the events to Event Bus, make sure you have the following:

		*	An Atom deployed on your local machine or hosted with Boomi Integration.
		*	Your AWS access key and AWS secret key.
		*	Amazon EventBridge access for your account if using an AWS Identity and Access Management (IAM) account (recommended).
		*	Event buses and rules set up on your Amazon EventBridge service (minimal implementation requires the default event bus and one rule specified with a target service).
		*	User can point the Event Bridge to the Cloud Watch Logs inorder to verify the sent Events to the event bridge.
		*	Dell Boomi Username and password with the necessary credentials.
		
		
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

# Supported Operation #

The Amazon EventBridge operations defines how to interact with your AWS account and represents a specific action (Create) to be performed against a specific file in JSON format.
Create a separate operation component for each action/object combination that your integration requires.
The Amazon EventBridge operations supports following actions:
* Create — The outbound operation which create events in the Amazon EventBridge (Put Events) defined in the operation.

## Operation Details ##

## Create ##

Create is an outbound action that you can Create a new event(s) to the Event Bridge in the amazon services.

### Connector Action ###
To determine whether you are creating an operation related to inbound, specify the proper connector action. This allows you to import the associated request and response JSON profiles.

Depending on how you create the operation component, the action type is either configurable or non-configurable from the drop-down list. The available action listed for this connector currently is Create.

### Object ###
Defines the object with which you want to integrate, and which is selected in the Import Wizard.

### Request Profile ###
Select or add an JSON profile component that represents the structure that is being sent by the connector.

### Response Profile ###
Select or add an JSON profile component that represents the structure that is being received by the connector.

# User Guide #

Find the below link to the user guide

https://help.boomi.com/bundle/connectors/page/int-Amazon_EventBridge_connector.html
