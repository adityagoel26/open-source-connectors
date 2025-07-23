# SAP S4 OData API
The SAP S4 OData v2 connector supports a rich set of features including deep queries to join child object types, support for E-Tag version control and the X-CSRF security tokens.

For more information regarding SAP S4 OData APIs please refer to the following
 * https://help.sap.com/viewer/search?state=PRODUCTION&language=en-US&format=standard,html,pdf,others&advAll=&advAny=&advPhr=odata%20api%20for&advNot=
 * Business Partner - https://help.sap.com/viewer/44e06f22436c43e582db6ccd5250e29b
 * OData API for Seasons - https://help.sap.com/viewer/e296651f454c4284ade361292c633d69
 * OData API for Service Order  - https://help.sap.com/viewer/085edb30fb3d413da552832f3d5c01c0
 
Instructions for uploading private connectors to your account are here: https://help.boomi.com/bundle/connectors/page/t-atm-Adding_a_connector_group.html . Download connector descriptor (xml) and archive (zip) files from [HERE](../../downloads).

Outstanding Topics:
* Function imports have not been tested. Questions remain about whether etag and other headers are required? Also singleton/array return types need to be verified. A sample function import is documented here: https://help.sap.com/viewer/9f047b05da4545ca8f9ebfc22acefd06/2020.000/en-US/d6456a46ed7a41e0bbde173e83342569.html 
* Are the SAP $metadata updateable=false and createable=false to filter fields from edit request profiles usable? This would prevent writing read only fields like "last modified by" but if the values are inaccurate then that would be bad to exclude them from the profile
* Are the filterable=false, sortable=false attributes accurate enough that we could use them to exclude fields from Filter, Sort? For now trying to sort/filter on a field that is not supported will throw an error
* **TODO** Complex Types are not supported. An exception will be thrown during import if any Complex types are encountered in an EntityType
* **TODO** $batch not supported (see below)
 
# Session Cookie and X-CSRF Token Handling

These values are required for edit operations including Create, Update/Post, Update/Put and Delete. They are captured from responses of all Get operations and persisted to dynamic process properties. The same dynamic process properties allow setting of these values when performing edit operations. 

These values are also captured automatically for edit operations when the *Capture Headers* option is specified for the operation by executing a Get operation. For Create operations, the Get operation is performed on the service root and executed only once for a batch of documents. For other edit operations, they are captured for each individual Get of each record being edited for the same operation used to capture the ETag value.

# ETag/Entity Version Number Handling

The ETag value is required for Update/Post, Update/Put and Delete edit operations. It is captured from responses of all Get operations and persisted a dynamic process property. The same dynamic process property allow setting of these values when performing edit operations. 

These values are also captured automatically for edit operations when the *Capture Headers* option is specified for the operation by performing a Get of each record prior to performing the edit operation. If the ETag value is not found in the response header, the response payload is checked for an ETag element at the root level.

# Create with Deep Payloads
The ability to create a parent entity type and related child entities is allowed for SAP S4 entity types that allow it. During Import the user selects a "depth" value and the appropriate hierarchical request profile is generated. 

**??QUESTION??** Does S4 process the deep payload transactionally? Meaning if any segment fails, are all entity edits rolled back?

For more info please refer to: https://help.sap.com/viewer/44e06f22436c43e582db6ccd5250e29b/2020.000/en-US/cfb5c8a12cd44100a9ea939ce6a23118.html

# Profile Generation during Import
There are specific features in profile field generation:

* Field descriptions are captured from "quickinfo" metadata attributes
* Native JSON formats are used in lieu of native OData formats. Conversions occur automatically during operation execution. 
* Datetime, DateTimeOffset and Time are handled by JSON strings with the appropriate format specifications. 
* Single, Double and decimal all correspond to number format. 
* Int64, Int32 and Byte correspond to integer formats. Note that both integer and number are handled by the Boomi number type. 
* The maximum length attribute is captured for String types. It is hard coded to 36 for GUID types.
* GUID is handled as a JSON string
* Binary is handled as a Base64 encoded JSON String
* **Question** specify string minimum length? Will this force a string to be inadvertently required?
* **Question** use NULLABLE attribute to specify the JSON "required" attribute, assuming Boomi JSON profile import supports that?

# Processing Documents with $Batch

**TODO The $batch odata operation is not yet supported.**

This operation avoids executing an http transaction for each inbound document. Instead each http transaction includes a batch of documents as specified by the *Batch Size* parameter. This improves performance and lessens the load on the server. By default each document is processed in the batch independently, not as a single transaction. If an error occurs with a single documents, the other documents will still be processed. 

$batch is also useful for Query operations where a large number of individual fields are selected and results in a "414 URI Too Long" error. Note this error can also be avoided turning of the **Allow Field Selection** option for the Query operation.

$batch also allows grouping of deep edit operations as individual transactions. 

# Navigation Property/Link handling

Navigation Properties aka Links are how relationships between related entities are expressed. For example an Order can have a 1:Many navigation property to Order Items. An Order can also have a M:1 relationship with a Customer entity. 

Update/Put and Update/Patch request profiles provide fields for the keys of child items. Mapping to these key fields enable "linking" to existing entities as specified by those key values.

For Create operations the **Link to Existing Children** mode provides a request profile with fields for the keys of child items. Mapping to these key fields enable "linking" to existing entities as specified by those key values. **Create Children** provides a deep profile so fields values can be specified to create the parent item and child items in a single operation. For example, the A_BusinessPartner entity type supports 'deep payloads' for creating the business partner and child items with a single request.

**TODO** we may need a hybrid mode where, for example we can create items for a new order but link to an existing customer?
**TODO** Create Children with Batches mode will support deep creates for when the API does not support the mode for an entity. In this case the parent element will be created and each child element will be created in a single batch transaction aka 'change set'.

# Connection Tab

## Connection Fields


### Service Server URL

The URL for the OData Server. Note this value will prefix any service value selected.

**Type** - string




### SAP/S4 Service Catalog

Choose a service to use from the list of SAP/S4 services

**Type** - string

#### Allowed Values

 * Activity Type Plan Cost Rates - ACCOSTRATE (C_ACTYTYPEPLANCOSTRATE_SRV_0001)
 * Bill Of Material Comparison (API_BILLOFMATERIAL_COMPARISON_SRV_0001)
 * Bill of Material CRUD (API_BILL_OF_MATERIAL_SRV_0001)
 * Bill of Material CRUD V2 (API_BILL_OF_MATERIAL_SRV_0002)
 * Bill of Material Where Used (API_BOM_WHERE_USED_SRV_0001)
 * Billing Document (API_BILLING_DOCUMENT_SRV_0001)
 * Business Area (API_BUSINESSAREA_SRV_0001)
 * Business Partner (API_BUSINESS_PARTNER_0001)
 * Catalog Service Version v2 (/IWFND/SG_MED_CATALOG_0002)
 * Change Master (API_CHANGEMASTER_0001)
 * Change Master v2 (API_CHANGEMASTER_0002)
 * Characteristic Attribute Catalog (API_CHARCATTRIBUTECATALOG_SRV_0001)
 * Chart of accounts (API_CHARTOFACCOUNTS_SRV_0001)
 * Company Code (API_COMPANYCODE_SRV_0001)
 * Controlling Debit Credit Code (API_CONTROLLINGDEBITCREDITCODE_SRV_0001)
 * ControllingArea (API_CONTROLLINGAREA_SRV_0001)
 * Cost Center (API_COSTCENTER_SRV_0001)
 * Cost Center Activity Type (API_COSTCENTERACTIVITYTYPE_SRV_0001)
 * Cost Center Data Provider (FCO_PI_COST_CENTER_0001)
 * Cost Center Hierarchy Node (C_COSTCENTERHIERARCHYNODE_SRV_0001)
 * Cost Estimate Items (API_FINPLAN_COSTESTIMATE_ITEMS_SRV_0001)
 * Country (API_COUNTRY_SRV_0001)
 * Credit Memo Request (API_CREDIT_MEMO_REQUEST_SRV_0001)
 * Customer (MD_CUSTOMER_MASTER_SRV_01_0001)
 * Customer Group (API_CUSTOMERGROUP_SRV_0001)
 * Customer Material (API_CUSTOMER_MATERIAL_SRV_0001)
 * Customer Return (API_SALES_QUOTATION_SRV_0001)
 * Customer Returns Delivery (API_CUSTOMER_RETURNS_DELIVERY_SRV_0001)
 * Customer Returns Delivery v2(API_CUSTOMER_RETURNS_DELIVERY_SRV_0002)
 * Customer Supplier Industry (API_CUSTOMERSUPPLIERINDUSTRY_SRV_0001)
 * Debit Memo Request (API_DEBIT_MEMO_REQUEST_SRV_0001)
 * Defect (API_DEFECT_SRV_0001)
 * Defect Category (API_DEFECTCATEGORY_SRV_0001)
 * Defect Class (API_DEFECTCLASS_SRV_0001)
 * Defect Code (API_DEFECTCODE_SRV_0001)
 * Distribution Channel (API_DISTRIBUTIONCHANNEL_SRV_0001)
 * Division (API_DIVISION_SRV_0001)
 * DMS (API_DMS_PROCESS_SRV_0001)
 * Employee Service (/SHCM/EMPLOYEE_SRV_0001)
 * Employee Service Cost Rate Level (SERVICE_COSTRATE_LEVEL_SRV_0001)
 * External Services for Delaware (/CPD/SC_EXTERNAL_SERVICES_SRV_0001)
 * FIN Project Definition Details (API_FINPROJECT_SRV_0001)
 * FIN Unit Of Measure (C_FINUNITOFMEASURE_SRV_0001)
 * FIN WBS Element Details (API_FINWBSELEMENT_SRV_0001)
 * FIN WBS Element Hierarchy Node (C_FINWBSELEMENTHIERARCHYNODE_SRV_0001)
 * Financial Planning Entry Item (API_FINPLANNINGENTRYITEM_SRV_0001)
 * Functional Area (API_FUNCTIONALAREA_SRV_0001)
 * G/L Account (API_GLACCOUNTINCHARTOFACCOUNTS_SRV_0001)
 * G/L Account Balance with Flow - Measure (C_GLACCOUNTFLOW_SRV_0001)
 * G/L Account Hierarchy Node (C_GLACCOUNTHIERARCHYNODE_SRV_0001)
 * G/L AccountLineItem (API_GLACCOUNTLINEITEM_0001)
 * Gateway Attachment Service (API_CV_ATTACHMENT_SRV_0001)
 * Gateway Project for Functional Area Hierarchy View (C_FUNCTIONALAREAHIERNODE_SRV_0001)
 * Inbound Delivery (API_INBOUND_DELIVERY_SRV_0001)
 * Inbound Delivery v2 (API_INBOUND_DELIVERY_SRV_0002)
 * Inspection Plan (API_INSPECTIONPLAN_SRV_0001)
 * Internal Order (API_INTERNALORDER_SRV_0001)
 * Journal Entry Item (API_JOURNALENTRYITEMBASIC_SRV_0001)
 * Journal Entry Item Basic View (I_JOURNALENTRYITEMBASIC_CDS_0001)
 * KPIs based on G/L Account Flow (C_GLACCOUNTFLOWKPI_CDS_0001)
 * Lean Service for Writing Plan Data to ACDOCP (API_FINPLANNINGDATA_SRV_0001)
 * Ledger (API_LEDGER_SRV_0001)
 * Manage Workforce Timesheet API (API_MANAGE_WORKFORCE_TIMESHEET_0001)
 * Master Inspection Characteristic (API_MASTERINSPCHARACTERISTIC_SRV_0001)
 * Material Valuation (API_MATERIAL_VALUATION_SRV_0001)
 * Opening Balance (API_OPENINGBALANCE_SRV_0001)
 * Operational Accounting Document Header and Item (API_OPLACCTGDOCITEMCUBE_SRV_0001)
 * Outbound Delivery (API_OUTBOUND_DELIVERY_SRV_0001)
 * Outbound Delivery v2 (API_OUTBOUND_DELIVERY_SRV_0002)
 * Physical Inventory API (API_PHYSICAL_INVENTORY_DOC_SRV_0001)
 * Planning - ACDOCP (API_FINANCIALPLANDATA_SRV_0001)
 * Planning Category (API_PLANNINGCATEGORY_SRV_0001)
 * Plant (API_PLANT_SRV_0001)
 * Product Hierarchy Node (MD_PRODUCT_HIERARCHY_SRV_0001)
 * Product Group (API_PRODUCTGROUP_SRV_0001)
 * Product Master (API_PRODUCT_SRV_0001)
 * Profit Center (API_PROFITCENTER_SRV_0001)
 * Profit Center Hierarchy Node (C_PROFITCENTERHIERNODE_SRV_0001)
 * Project Engagement(/CPD/SC_PROJ_ENGMT_CREATE_UPD_SRV_0001)
 * Public API - Availability (API_MANAGE_WF_AVAILABILITY_0001)
 * Purchase Contract API (API_PURCHASECONTRACT_PROCESS_SRV_0001)
 * Purchase Order (API_PURCHASEORDER_PROCESS_SRV_0001)
 * Quality Inspection Method (API_INSPECTIONMETHOD_SRV_0001)
 * RAP: UI_FINS_GLACCOUNTFLOW generated at 20190213 (UI_FINS_GLACCOUNTFLOW_0001)
 * Request for Quotation (API_RFQ_PROCESS_SRV_0001)
 * Sales District (API_SALESDISTRICT_SRV_0001)
 * Sales Order (API_SALES_ORDER_SRV_0001)
 * Sales Order Bill of Material CRUD API (API_ORDER_BILL_OF_MATERIAL_SRV_0001)
 * Sales Order Simulation (API_SALES_ORDER_SIMULATION_SRV_0001)
 * Sales Organization (API_SALESORGANIZATION_SRV_0001)
 * Segment (API_SEGMENT_SRV_0001)
 * Statistical Key Figures - FINSSKF (C_SKF_SRV_0001)
 * Supplier Invoice (API_SUPPLIERINVOICE_PROCESS_SRV_0001)
 * Supplier Master (MD_SUPPLIER_MASTER_SRV_0001)
 * Supplier Quotation (API_QTN_PROCESS_SRV_0001)
 * Trading Partner (API_PARTNERCOMPANY_SRV_0001)
 * Trial Balance (C_TRIALBALANCE_CDS_0001)



### Alternate Service URL

Enter the full URL of the Server and Service. This will override selections from the catalog so you can provide a full url to any metadata service.

**Type** - string




### AuthenticationType

 **helpText NOT SET IN DESCRIPTOR FILE**

**Type** - string

**Default Value** - BASIC

#### Allowed Values

 * None
 * Basic
 * OAuth 2.0



### User Name

User name for Basic Authentication. Leave blank for other authenticate types.

**Type** - string




### Password

Password for Basic Authentication. Leave blank for other authenticate types.

**Type** - password




### OAuth 2.0

 **helpText NOT SET IN DESCRIPTOR FILE**

**Type** - oauth


# Operation Tab



## GET


## QUERY
### Operation Fields



#### Maximum Browse Depth

Specifies the maximum depth to include child objects in the profile

**Type** - integer

**Default Value** - 0




#### Include recursive navigations

Include deep navigations to that circle back to parent object types types. For example allow Order->Order_Details->Order.

**Type** - boolean

**Default Value** - false




#### Page Size

Specifies the number of documents to retrieve with each page transaction.

**Type** - integer

**Default Value** - 20




#### Maximum Documents

Limits the number of documents returned. If value is less than 1, all records are returned.

**Type** - integer

**Default Value** - -1




#### Allow Field Selection

Allow individually fields to be selected. Otherwise field selection is only to choose what related child entities to include in the response along with all the fields for each selected entity. Specifying individual fields to return requires large API calls and should only be used when relatively few fields are required.

**Type** - boolean

**Default Value** - false



#### Query Options


##### Fields

Use the checkboxes in the *Fields* list to select which fields are returned by the Query operation. Selecting only the fields required can improve performance.


##### Filters

 The query filter supports any arbitrary grouping and nesting of AND's and OR's.

Example:
((foo lessThan 30) OR (baz lessThan 42) OR ((bar isNull) AND (bazz isNotNull))) AND (buzz greaterThan 55)

##### Filter Operators

 * Equal To Example /Suppliers?$filter=Address/City eq 'Redmond'

 * Not Equal To  **helpText NOT SET IN DESCRIPTOR FILE**

 * Greater Than  **helpText NOT SET IN DESCRIPTOR FILE**

 * Greater Than Or Equal  **helpText NOT SET IN DESCRIPTOR FILE**

 * Less Than  **helpText NOT SET IN DESCRIPTOR FILE**

 * Less Than Or Equal  **helpText NOT SET IN DESCRIPTOR FILE**

 * Ends With  (Supported Types:  string)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Starts With  (Supported Types:  string)  **helpText NOT SET IN DESCRIPTOR FILE**

 * Contains  (Supported Types:  string)  **helpText NOT SET IN DESCRIPTOR FILE**



##### Sorts



The sort order can be set to either ascending and descinding.


## CREATE
### Operation Fields



#### Capture Headers

Execute a GET operation to capture headers prior to performing the edit operation. This eliminates the need to manage the connector X-CSRF Token and Session Cookie Document Properties manually.

**Type** - boolean

**Default Value** - true




#### Maximum Entity Depth

Specifies the maximum depth to include child objects in the profile. This enables the implementation of 'Create with Deep Payload'

**Type** - integer

**Default Value** - 0




## UPDATE/PATCH/MERGE
### Operation Fields



#### Capture Headers

Execute a GET operation to capture headers prior to performing the edit operation. This eliminates the need to manage the connector ETag, X-CSRF Token and Session Cookie Document Properties manually. Note ETag values in the GET response body will also be captured.

**Type** - boolean

**Default Value** - true




## UPDATE/PUT
### Operation Fields



#### Capture Headers

Execute a GET operation to capture headers prior to performing the edit operation. This eliminates the need to manage the connector ETag, X-CSRF Token and Session Cookie Document Properties manually. Note ETag values in the GET response body will also be captured.

**Type** - boolean

**Default Value** - true




## DELETE
### Operation Fields



#### Capture Headers

Execute a GET operation to capture headers prior to performing the edit operation. This eliminates the need to manage the connector X-CSRF Token and Session Cookie Document Properties manually.

**Type** - boolean

**Default Value** - true




## EXECUTE FUNCTION IMPORT

# Inbound Document Properties
Inbound document properties can set by a process before a connector shape to control options supported by the connector.

 * ETag/Document Version Number (Required for UPDATE operations)
 * X-CSRF Token (Required for write operations)
 * SAP_SESSIONID_COOKIE (Required for write operations)

# Outbound Document Properties

Outbound document properties can used by a process after a connector shape to access information set by the connector.

 * ETag/Document Version Number (Returned by GET operations)
 * X-CSRF Token (Returned by GET operations)
 * SAP_SESSIONID_COOKIE (Returned by GET operations)
