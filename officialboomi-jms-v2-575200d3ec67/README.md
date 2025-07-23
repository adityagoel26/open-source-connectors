# JMS V2 connector #
Use the JMS v2 connector to work with either JMS v1.1 or 2.0 to send and receive messages from JMS queues or topics. 
The JMS v2 connector supports transactions, durable/non-durable topic subscriptions, configurable receive timeouts, 
message selectors, and custom message properties.

To set up the connector, you must obtain your JMS provider’s JAR files and place those files in the appropriate 
directory. You can do that in one of two ways:

* Upload the JAR files into your Integration account library (Setup > Account Libraries), add those files to a 
Custom Library component, and deploy the component to the appropriate Atom, Molecule, Atom Cloud, or environment.  
JAR files that support a connector, you create a Custom Library component with a type of Connector and 
select a connector type — in this case, JMS v2. When you deploy the component, the JAR files that it references are
deployed to the /<installation_directory>/userlib/jms directory. If the userlib/jms directory does not exist, 
Integration creates it.

* Load the JAR files manually into the appropriate directory of a local Atom, Molecule, or private Atom Cloud. After 
you load the JAR files, a restart is required. Any driver files that you load manually must be copied into 
the /<installation_directory>/userlib/jms directory. If the userlib/jms directory does not exist, you must create it.


> **_NOTE:_** Uploaded or imported files are first passed through a virus scanner. If a virus is detected, the upload 
> or import results in an error and the file is rejected. Contact Boomi Support if an error persists.

Make sure that the Atom can reach the /userlib/jms directory (the permissions are correct). The JMS v2 connector 
inspects this directory and uses any JAR files it finds there.


## Connector configuration ##

---
To configure a connector to communicate with your JMS provider, set up two components:

* JMS v2 connection
* JMS v2 operation

This design provides reusable components, which contain connection settings such as user name, password, etc. After 
building your connection and connector operation, set up your connector within a process. When you have properly 
configured your connector within your process, Integration can map to and from virtually any system using the JMS v2 
connector to send and receive data from your JMS provider.

## Supported editions ##

---
The JMS v2 connector works with both JMS v1.1 and v2.0 to send and receive messages from JMS queues or topics.

## Tracked properties ##

---
This connector has the following tracked properties that can be set or referenced in various shape parameters. You 
can define a maximum of 20 tracked fields, and each tracked field's value is limited to 1000 characters. If the 
tracked field's value is longer than 1000 characters, when creating and storing the value, the platform truncates 
the value. See the topic [Document Tracking](https://help.boomi.com/csh?topicname=c-atm-Document_Tracking.html) for 
more information.

**Name**  

**Description**  

**Correlation ID**  
The value of the JMSCorrelationID header field. Use the JMS Correlation ID to link one message with another.

**Custom Properties**  
An overridable operation property allowing users to set custom properties to include in sent messages with key/value 
pairs.

**Destination**  
The value of the JMSDestination header field which defines the name of the destination to which the message was sent.

**Expiration Time**  
A tracked property that represent the time in milliseconds between Expiration Time and midnight, January 1st, 1970 UTC.

**Message ID**  
The value of the JMSMessageID header field which uniquely identifies each message sent by a provider.

**Message Type**  
The value of the JMSType header field which is used for freestyle text, such as Invoice or Order. The field does not 
reference the Message Type selected in the JMS Send operation.

**Message Class**  
The value, which is set upon completion of a Get/Listen operation, contains one of three message classes corresponding 
to the message types currently supported: BytesMessage, TextMessage, and MapMessage. The Message Class tracking 
property routes process logic to the branch logic to handle the specific class of message received.

To process a received message as a MapMessage, set the Document Property Message Class to MapMessage. MapMessage 
supports the Java primitive data types (Byte, Short, Integer, Long, Float, Double, Character, String, Boolean), which 
are case-sensitive, and the byte [ ] array. The input values should be compatible with the Java type.valueOf(String input) 
method; however, the byte array is a Base64 encoded sequence.

**Priority**  
The value of the JMSPriority header field. There are 10 priority values (0 is the lowest, 9 is the highest).

**Redelivered**  
The value of the JMSRedelivered header field which indicates that the message was delivered earlier, but its receipt 
was not acknowledged at that time.

**Reply To**  
The value of the JMSReplyTo header field which indicates a topic or queue where the reply to the current message 
should be sent.

# Connection #

---
The JMS v2 connection contains all of the information that is needed to connect to a specific JMS provider. The 
connection supports generic JNDI connections, as well as connections to some other common JMS providers.

The connector has been tested against the following common JMS providers:

* ActiveMQ Classic (compatible with JMS v1.1)
* ActiveMQ Artemis
* Generic JNDI
* Oracle AQ (compatible with JMS v1.1)
* Oracle AQ (WebLogic)
* SonicMQ
* IBM Websphere MQ
Each provider has its own setup steps, as well as specific JMS connector setup steps.

## Connection tab ##

---
The following fields appear on the Connection tab for all supported JMS providers:

**Name**  

**Description**  

**Use Authentication**  
A checkbox the user may select to use authentication.

**Username**  
A text field where the user may enter their username for authentication. This is a conditional field that appears when Authentication is selected.

**Password**  
A text field where the user may enter their password for authentication. This is a conditional field that appears when Authentication is selected.

**JMS Version**  
A dropdown where users may choose to use JMS v.1.1 or 2.0.

**Server Type**  
A dropdown listing all the services supported by the connector.

# Supported Operations #

---
The JMS v2 operation defines how to interact with the connection, including transactions, batching, custom properties, etc.

The operation supports these actions:

* Send
* Get
* Listen

## Browsing ##

---
**Dynamic Destination** is the only object type available for all operations. The destination type desired by the user 
can be configured on the browse screen with the dropdown field **Server Type**.

For the Send operation, the connector will generate an input profile depending on the Destination Type selected by the 
user. The output profile will always be in JSON, containing the fields **Message ID**, **Destination**, and **Destination Type**.

For **Receive** and **Listen** operations, the connector will generate output profiles depending on the 
**Destination Type** selected by the user.

The following fields appear on the Browse tab:

**Name**  

**Description**  

**Destination Type**  
A dropdown box allowing the user to configure the type of messages accepted by the destination so an appropriate 
profile may be generated.
* Text Messages
* Text Messages (XML)
* Map Messages
* Byte Messages
* XMLType Messages (OracleAQ only)
* Object Messages (OracleAQ only )

**Dynamic Destination**  
A conditional dropdown box that is visible when **OracleAQ** is chosen for the **Server Type**. When selected, the 
connector will not retrieve destinations from the service.

**Server Type**  
A dropdown box allowing to user to retrieve the destination list from a static list or through an **OracleAQ** connection.

## Send ##

---
This operation produces messages to the configured JMS service. The payload of these messages is the input document, 
with the exception of Map Messages where the payload is a JSON document with an array of objects.

As a safety measure, the operation will avoid sending messages whose size is greater than the threshold defined by the 
container property `com.boomi.connector.jmsv2.max.message.size`. The default value for this configuration is 1 MB.

**Name**  

**Description**  

**Destination**  
An importable text field allowing the user to select the desired queue or topic where messages will be sent. This field 
becomes read-only when **OracleAQ** is selected as the **Dynamic Destination**

**Use Transaction**  
A checkbox indicating if messages will be sent within a transaction.

**Transaction Batch Size**  
A conditional text field that is visible when **Use Transaction** is checked. The default value is 1.

**Destination Type**  
An importable dropdown box listing all the destination types available. The value chosen at browse time is selected 
by default.

**Custom Properties**  
An overridable operation property allowing users to set custom properties to include in sent messages with key/value 
pairs.

**Correlation ID**  
An overridable operation property.

**Message Type**  
An overridable operation property.

**Message Class**  
An overridable operation property.

**Priority**  
An overridable operation property.

**Reply To**  
An overridable operation property.

**Time to Live**  
An overridable field that represents the time a given message will expire in milliseconds. Input to this field must be 
greater than zero; if the input is exactly zero, the message will not expire.

## Get ##

---
This operation fetches messages from a queue or topic subscription within the configured service. The payload of these 
messages is the output document, with the exception of Map Messages where the payload is a JSON document built from 
the key/value pairs contained in Map Message.

**Name**  

**Description**  

**Destination**  
An overridable operation property that indicates the queue or topic messages will be fetched from.

**Destination Type**  
An importable dropdown box listing all the destination types available.

**Use Transaction**  
A checkbox indicating if messages will be received within a transaction.

**Message Selector**  
A query filter used to filter received messages using the JMS Consumer.

**Durable Subscription**  
A checkbox indicating if the subscription associated with a topic is durable. Only used for topics.

**Subscription Name**  
A conditional text field that is visible when **Durable Subscription** is visible where the user can enter the name of the 
durable subscription.

**Receive Mode**  
A dropdown that allows the user to choose how they want to receive messages.
* No Wait
* Limited Number of Messages
* Limited Number of Messages with Timeout
* Unlimited Number of Messages with Timeout

**Maximum Number of Messages**  
A conditional text field that is visible when Receive Mode is set to Limited Number of Messages or Unlimited Number of 
Messages with Timeout. Input to this field indicates how many messages the connector will retrieve before completing 
execution and must be a number greater than zero.

**Receive Timeout**  
A conditional text field that is visible when **Receive Mode** is set to **Limited Number of Messages** with 
**Timeout** or **Unlimited Number of Messages with Timeout**. Input to this field indicates how many messages the 
connector will retrieve before completing execution and must be a number greater than zero.

**Expiration Time**  
A tracked property that represent the time in milliseconds between **Expiration Time** and midnight, January 1st, 1970 UTC.

**Correlation ID**  
An output document property.

**Message ID**  
An output document property.

**Message Type**  
An output document property.

**Message Class**  
An output document property.

**Priority**  
An output document property.

**Redelivered**  
An output document property.

**Reply To**  
An output document property.

## Listen ##

---
This operation retrieves messages from a queue or topic within the configured JMS service and submits them 
individually for processing. The payload of these messages is the output document, with the exception of Map Messages 
where the payload is a JSON document containing the key/value pairs present in the message.

**Name**  

**Description**  

**Destination**  
An overridable operation property that indicates the queue or topic messages will be fetched from.

**Delivery Policy**  
A dropdown that indicates the delivery policy used by the operation.
* At least once
* At most once

**Use Transaction**  
A checkbox indicating if messages will be received within a transaction.

**Message Selector**  
A query filter used to filter received messages using the JMS Consumer.

**Durable Subscription**  
A checkbox indicating if the subscription associated with a topic is durable. Only used for topics.

**Subscription Name**  
A conditional text field that is visible when *Durable Subscription* is visible where the user can enter the name of 
the durable subscription.

**Maximum Concurrent Executions**  
A text field indicating how many consumers the connector will subscribe to in order to receive messages.

**Singleton Listener**  
A checkbox that when checked, determines that when the Listen operation is deployed to a multi-node environment, the 
operation will be executed in only a single node.

**Expiration Time**  
A tracked property that represent the time in milliseconds between **Expiration Time** and midnight, January 1st, 1970 UTC.

**Correlation ID**  
An output document property.

**Message ID**  
An output document property.

**Message Type**  
An output document property.

**Message Class**  
An output document property.

**Priority**  
An output document property.

**Redelivered**  
An output document property.

**Reply To**  
An output document property.

# Building this connector project #

---
To build this project, the following dependencies must be met

* JDK 1.8 or above

### Compiling ###
Once your JAVA_HOME points to your installation of JDK 1.8 (or above) and JAVA_HOME/bin are in your PATH, issue the 
following command to build the jar file:
```
  ./gradlew clean build
```
or if you don't want to run the unit tests, then run
```
  ./gradlew clean build -x test
``` 
### Running the unit tests ###
To run the unit tests, please use below command
``` 
  ./gradlew test 
```
It will run the unit tests and build the jar file. Both the CAR file and the Test Reports will be available inside 
build folder.

## Additional Resources ##

---
https://en.wikipedia.org/wiki/Java_Message_Service