# Veeva Connector

Veeva is a leader in document management for the health sciences space. Common usecases of Veeva Vault are pharma companies managing documents for clinical trials and FDA approval.

This connector implements a subset of the Veeva REST API: https://developer.veevavault.com/docs/#rest-api
The connector artifacts are in the Downloads folder. Instructions for installation in your Account are at https://help.boomi.com/bundle/connectors/page/t-atm-Adding_a_connector_group.html

Note only create one connector group and one connector. You can upload versions multiple versions to a single connector group/connector.  Warning: extra Group and extra Connectors CANNOT be deleted, hence these guidelines.

The connector is designed to CRUD multiple object types exposed by the API:
 * Document
 * VObject - a generic type enabling custom objects
 
 All of these object types include custom fields, hence Import/Browse functionality uses Veeva metadata apis to implement querying object types and generating request and response profiles.
 
 The document object represents the metadata for actual documents. Documents are managed via upload/download APIs. APIs provide the ability to manage individual documents directly but for scalability batching is implemented by APIs that asynchronously transfer documents to FTP servers or Veevas File Staging Server: 
 
 https://developer.veevavault.com/docs/#using-the-file-staging-server 

This connector provides a Boomi Query Operation to enable the powerful VQL query language for these object types. This includes typical query capabilities including field selection, filtering, sorting and results pagination. 

https://developer.veevavault.com/api/19.2/#vault-query-language-vql

Although Veeva is a RESTful API, Veeva does not provide an OpenAPI Specification file. A major reason for this is custom objects and custom fields are supported in the platform. Still, in order to minimize code, the connector levarages SDK OpenAPI framework where possible. A hand-crafted OpenAPI spec implements much of the connector functionality: 

https://bitbucket.org/officialboomi/veeva/src/master/src/main/resources/openapi.json

Since the SDK only supports JSON, code to implement other MIME types is included. This code can be removed as the SDK is enhanced.

# VQL Joins of Related objects

The connector supports all flavors of joining according to https://developer.veevavault.com/vql/#object-to-object-relationships

## Left Outer Join - Parent to Child (1:M)
These are implemented by generating a sub-query in the VQL when child field checkboxes are selected in the Query Operation - Fields subpanel. Note these will only appear if the Veeva object metadata indicates relationship_type==child
https://developer.veevavault.com/vql/#left-outer-join-parent-to-child-1-m

## Inner Join - Parent to Child (1:M)
This is implemented by using the Contains(IN) filter operation. The value for the filter being a SELECT statement returning IDs from the related object.
https://developer.veevavault.com/vql/#inner-join-parent-to-child-1-m

## Inner Join - Child to Parent (M:1)
These are implemented by using a . delimiter for the full path of the field name in a SELECT. Selecting field checkboxes will include fields in the select statement with the .
https://developer.veevavault.com/vql/#inner-join-child-to-parent-m-1

## Lookup
These join is implemented by simply building Filters and choosing a child field for the filter. The full path of the field will be qualified with a . delimiter. 
https://developer.veevavault.com/vql/#lookup

# Connection Tab

## Connection Fields

### Veeva API Server

The endpoint of the Boomi API server. Subdomains indicate different Veeva tenants: https://developer.veevavault.com/docs/#structuring-the-endpoint

**Type** - string

**Default Value** - https://XXXXXX.veevavault.com/api/v23.2




### User

 **helpText NOT SET IN DESCRIPTOR FILE**

**Type** - string




### Password

 **helpText NOT SET IN DESCRIPTOR FILE**

**Type** - password




### Session Cache Scope

Choose between caching a session for the duration a process execution, or cache on the Atom for sharing across process executions

**Type** - string

**Default Value** - EXECUTION

#### Allowed Values
 * Single Process Execution
 * All Process Executions



### Session Timeout (hours)

Indicates when to refresh a session token that is cached for multiple process executions. Maximum is 24 hours

**Type** - integer

**Default Value** - 12


# Operation Tab



## CREATE/EXECUTE
### Operation Fields



#### Object Type

Each Vault Object has a unique set of fields. Select from a list of Vault Objects on the next page.

**Type** - string

**Default Value** - OBJECT_TYPE_VOBJECT

##### Allowed Values
 * Create Vault Objects
 * Create/Execute Other Types



#### Include Only Required Fields

Limit the number of fields in the request profile by including only required fields for Vault Object and Document create/update operations

**Type** - boolean

**Default Value** - true




## UPDATE
### Operation Fields



#### Object Type

Each Vault Object has a unique set of fields. Select from a list of Vault Objects on the next page.

**Type** - string

**Default Value** - OBJECT_TYPE_VOBJECT

##### Allowed Values
 * Update Vault Objects
 * Other Update/Put Types



#### Include Only Required Fields

Limit the number of fields in the request profile by including only required fields for Vault Object and Document create/update operations

**Type** - boolean

**Default Value** - true




## GET


## QUERY
### Operation Fields



#### Page Size

Specifies the number of documents to retrieve with each page transaction.

**Type** - integer

**Default Value** - 20




#### Maximum Documents

Limits the number of documents returned. If value is less than 1, all records are returned.

**Type** - integer

**Default Value** - -1




#### Version Filter Options (Documents Only)

Choose which document versions to return. This option is not applicable to Vault Objects, it is supported for documents only.

**Type** - string

**Default Value** - LATEST_VERSION

##### Allowed Values
 * Latest Version
 * All Versions
 * Latest Version that Matches Filter



#### FIND/Keyword Search

Perform keyword searches in single quotes on documents and Vault objects: 'diabetes AND insulin OR Nyaxa' For more info: https://developer.veevavault.com/vql/#find

**Type** - string




#### Custom VQL Statement

Allows specifying raw VQL. Use this for complex SELECT/WHERE/FIND terms that can not be modeled with Fields/Filter/Order By panels below.

**Type** - string




#### Include System Fields

Include all system fields. Otherwise only a subset of system will be available for querying to simplify field selection. Note, including all systems fields may provide fields that are not queryable due to security permissions and selecting them will result in error.

**Type** - boolean




#### Enable VQL Joins

Allows performing VQL joins by presenting child fields for selection. For documents, system object references will be excluded.

**Type** - integer

**Default Value** - 0

##### Allowed Values
 * Disable Joins
 * Enable Single Level Joins


### Query Options


#### Fields

Use the checkboxes in the *Fields* list to select which fields are returned by the Query operation. Selecting only the fields required can improve performance.


#### Filters

 The query filter supports any arbitrary grouping and nesting of AND's and OR's.

Example:
((foo lessThan 30) OR (baz lessThan 42) OR ((bar isNull) AND (bazz isNotNull))) AND (buzz greaterThan 55)

#### Filter Operators

 * Equal To  (Supported Types:  string)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Equal To  (Supported Types:  date)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Equal To  (Supported Types:  number)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Not Equal To  (Supported Types:  string)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Not Equal To  (Supported Types:  date)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Not Equal To  (Supported Types:  number)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Greater Than  (Supported Types:  string)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Greater Than  (Supported Types:  date)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Greater Than  (Supported Types:  number)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Less Than  (Supported Types:  string)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Less Than  (Supported Types:  date)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Less Than  (Supported Types:  number)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Greater Than Or Equal  (Supported Types:  string)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Greater Than Or Equal  (Supported Types:  date)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Greater Than Or Equal  (Supported Types:  number)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Less Than Or Equal  (Supported Types:  string)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Less Than Or Equal  (Supported Types:  date)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Less Than Or Equal  (Supported Types:  number)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Contains one of a comma delimited list: value_1,value_2,value_3  (Supported Types:  string)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Between 2 values with strings in single quotes: 'value_1' AND 'value_2'  **helpText NOT SET IN DESCRIPTOR FILE**

 * Like: '%wildcard'  (Supported Types:  string)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Contains one of the results of a sub query. For example: SELECT id FROM document_product__vr  **helpText NOT SET IN DESCRIPTOR FILE**



#### Sorts



The sort order can be set to either ascending and descinding.

# Inbound Document Properties
The connector does not support inbound document properties that can be set by a process before an connector shape.


# Outbound Document Properties

The connector does not support outbound document properties that can be read by a process after a connector shape.

