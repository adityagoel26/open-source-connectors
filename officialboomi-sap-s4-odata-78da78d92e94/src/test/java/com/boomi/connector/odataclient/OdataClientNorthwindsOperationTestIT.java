// Copyright (c) 2018 Boomi, Inc.

package com.boomi.connector.odataclient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.testutil.ConnectorTester;
import com.boomi.connector.testutil.SimpleOperationResult;

/**
 * @author Dave Hock
 */
//Create/Get/Delete in that order
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class OdataClientNorthwindsOperationTestIT
{
	private static String odataPayload = "{\"Description\": \"Low fat milk\",\"Category\": {\"__deferred\": {\"uri\": \"https://services.odata.org/(S(wsql3bknhzpajxwwzymcpi01))/V2/OData/OData.svc/Products(1)/Category\"}},\"Price\": \"3.5\",\"Rating\": 3,\"Ingredients\": [{\"obj\": {\"key\": \"value\"},\"keys\": [],\"Amount\": 1,\"Name\": \"Lactose\"},{\"Amount\": 6,\"Name\": \"Fat\"},{\"Amount\": 2,\"Name\": \"Sugar\"}],\"DiscontinuedDate\": \"/Date(1582761600000)/\" ,\"CreatedTime\": \"PT19H54M36S\", \"Supplier\": {\"__deferred\": {\"uri\": \"https://services.odata.org/(S(wsql3bknhzpajxwwzymcpi01))/V2/OData/OData.svc/Products(1)/Supplier\"}},\"ID\": 1,\"__metadata\": {\"type\": \"ODataDemo.Product\",\"uri\": \"https://services.odata.org/(S(wsql3bknhzpajxwwzymcpi01))/V2/OData/OData.svc/Products(1)\"},\"ReleaseDate\": \"/Date(812505600000)/\",\"Name\": \"Milk\"}";
	private static String outputCookie = "{\"properties\":{\"/CreatedTime\":{\"type\":\"Time\"},\"/DiscontinuedDate\":{\"type\":\"DateTime\"},\"/ReleaseDate\":{\"type\":\"DateTime\"},\"/ProductID\":{\"isKey\":true,\"type\":\"Int32\"}}}";
	private static String inputCookie = "{\"properties\":{\"/ID\":{\"isKey\":true,\"type\":\"Int32\"}}}";
	private static String jsonPayload = "{\"ID\" : 1}";
	private static final String _baseURL = "https://services.odata.org";
    private static final String WRITABLE_SERVICE_URL = "/V2/(S(nrnhk01vufvkp2201gac4eyd))/OData/OData.svc/";
    private static final String SERVICE_URL = "/V2/Northwind/Northwind.svc/";
    private static final String objectTypeId = "Products";
    private static Long _productID;
    private static final String ordersCookie = "{\"properties\" : {\r\n" + 
    		"	\"/RequiredDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/Orders\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Order\",\r\n" + 
    		"		\"numChildren\": 14,\r\n" + 
    		"		\"isArray\": true\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/Orders/Order_Details/UnitPrice\": {\r\n" + 
    		"		\"type\": \"Decimal\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Territories/Region/RegionID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/Employees1/Photo\": {\r\n" + 
    		"		\"type\": \"Binary\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper/Orders/Customer\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Customer\",\r\n" + 
    		"		\"numChildren\": 11\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/Orders\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Order\",\r\n" + 
    		"		\"numChildren\": 14,\r\n" + 
    		"		\"isArray\": true\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper/ShipperID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/Employee1\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Employee\",\r\n" + 
    		"		\"numChildren\": 18\r\n" + 
    		"	},\r\n" + 
    		"	\"/ShippedDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/Employees1\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Employee\",\r\n" + 
    		"		\"numChildren\": 18,\r\n" + 
    		"		\"isArray\": true\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Product/Order_Details/Discount\": {\r\n" + 
    		"		\"type\": \"Single\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/ProductID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/EmployeeID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Order/OrderID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/Orders/Employee/Photo\": {\r\n" + 
    		"		\"type\": \"Binary\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/Territories/TerritoryID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"String\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Product/Supplier\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Supplier\",\r\n" + 
    		"		\"numChildren\": 12\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/Orders/Order_Details/ProductID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Order/Customer\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Customer\",\r\n" + 
    		"		\"numChildren\": 11\r\n" + 
    		"	},\r\n" + 
    		"	\"/OrderDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/Photo\": {\r\n" + 
    		"		\"type\": \"Binary\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Orders/Order_Details\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Order_Detail\",\r\n" + 
    		"		\"numChildren\": 5,\r\n" + 
    		"		\"isArray\": true\r\n" + 
    		"	},\r\n" + 
    		"	\"/OrderID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/Orders/Freight\": {\r\n" + 
    		"		\"type\": \"Decimal\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Orders/Employee\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Employee\",\r\n" + 
    		"		\"numChildren\": 18\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/CustomerDemographics/Customers\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Customer\",\r\n" + 
    		"		\"numChildren\": 11,\r\n" + 
    		"		\"isArray\": true\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Orders/Customer\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Customer\",\r\n" + 
    		"		\"numChildren\": 11\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/Orders/RequiredDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/CustomerDemographics/Customers/CustomerID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"String\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/Employees1/EmployeeID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper/Orders/OrderID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Order\",\r\n" + 
    		"		\"numChildren\": 14\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Orders/Employee/BirthDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/Employees1/HireDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Order/Employee/HireDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/Employee1/Photo\": {\r\n" + 
    		"		\"type\": \"Binary\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Photo\": {\r\n" + 
    		"		\"type\": \"Binary\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Order/Shipper/ShipperID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Product/Category/CategoryID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/UnitPrice\": {\r\n" + 
    		"		\"type\": \"Decimal\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/Employee1/EmployeeID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/Orders/OrderID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/Employee1/HireDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/Orders/OrderDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper/Orders/RequiredDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Product/Supplier/SupplierID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/Employee1/EmployeeID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Orders/Order_Details/Discount\": {\r\n" + 
    		"		\"type\": \"Single\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Order/Order_Details/Discount\": {\r\n" + 
    		"		\"type\": \"Single\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Shipper\",\r\n" + 
    		"		\"numChildren\": 3\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper/Orders/OrderDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/Orders/Shipper/ShipperID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Orders/Employee/Photo\": {\r\n" + 
    		"		\"type\": \"Binary\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Orders/Shipper\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Shipper\",\r\n" + 
    		"		\"numChildren\": 3\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Territories/TerritoryID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"String\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Order/Customer/CustomerID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"String\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Orders/Order_Details/OrderID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper/Orders/ShippedDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper/Orders/Order_Details/Discount\": {\r\n" + 
    		"		\"type\": \"Single\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/Orders/Freight\": {\r\n" + 
    		"		\"type\": \"Decimal\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Order/ShippedDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper/Orders/Shipper\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Shipper\",\r\n" + 
    		"		\"numChildren\": 3\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Territories/Employees/BirthDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper/Orders/Employee\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Employee\",\r\n" + 
    		"		\"numChildren\": 18\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/Employee1\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Employee\",\r\n" + 
    		"		\"numChildren\": 18\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/CustomerDemographics\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.CustomerDemographic\",\r\n" + 
    		"		\"numChildren\": 2,\r\n" + 
    		"		\"isArray\": true\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/Employees1/HireDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/Territories\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Territory\",\r\n" + 
    		"		\"numChildren\": 3,\r\n" + 
    		"		\"isArray\": true\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Order/Employee/Photo\": {\r\n" + 
    		"		\"type\": \"Binary\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper/Orders/Shipper/ShipperID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Orders/Shipper/ShipperID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Product/Order_Details\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Order_Detail\",\r\n" + 
    		"		\"numChildren\": 5,\r\n" + 
    		"		\"isArray\": true\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/HireDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Product/ProductID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/Orders/Order_Details/Discount\": {\r\n" + 
    		"		\"type\": \"Single\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/Orders/OrderDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Employee\",\r\n" + 
    		"		\"numChildren\": 18\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Employee\",\r\n" + 
    		"		\"numChildren\": 18\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Order/Order_Details/UnitPrice\": {\r\n" + 
    		"		\"type\": \"Decimal\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/Territories\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Territory\",\r\n" + 
    		"		\"numChildren\": 3,\r\n" + 
    		"		\"isArray\": true\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/BirthDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Product/Category/Picture\": {\r\n" + 
    		"		\"type\": \"Binary\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/HireDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Orders/Employee/EmployeeID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Order/Order_Details\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Order_Detail\",\r\n" + 
    		"		\"numChildren\": 5,\r\n" + 
    		"		\"isArray\": true\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/Employees1/Photo\": {\r\n" + 
    		"		\"type\": \"Binary\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper/Orders/Employee/BirthDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Freight\": {\r\n" + 
    		"		\"type\": \"Decimal\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper/Orders/Customer/CustomerID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"String\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/Orders/Customer/CustomerID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"String\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper/Orders/Employee/HireDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Orders/Employee/HireDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Orders\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Order\",\r\n" + 
    		"		\"numChildren\": 14,\r\n" + 
    		"		\"isArray\": true\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Territories/Region\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Region\",\r\n" + 
    		"		\"numChildren\": 2\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Order/Order_Details/ProductID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Territories/Employees/EmployeeID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Orders/Freight\": {\r\n" + 
    		"		\"type\": \"Decimal\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Order/Freight\": {\r\n" + 
    		"		\"type\": \"Decimal\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/CustomerDemographics/CustomerTypeID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"String\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/Orders/OrderDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/Orders/Employee\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Employee\",\r\n" + 
    		"		\"numChildren\": 18\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Employee\",\r\n" + 
    		"		\"numChildren\": 18,\r\n" + 
    		"		\"isArray\": true\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/Employee1/BirthDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/EmployeeID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Orders/Customer/CustomerID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"String\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Territories/Employees\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Employee\",\r\n" + 
    		"		\"numChildren\": 18,\r\n" + 
    		"		\"isArray\": true\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper/Orders/Freight\": {\r\n" + 
    		"		\"type\": \"Decimal\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/Orders/Employee/BirthDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Order/RequiredDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Orders/Order_Details/ProductID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/Orders/Shipper\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Shipper\",\r\n" + 
    		"		\"numChildren\": 3\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Customer\",\r\n" + 
    		"		\"numChildren\": 11\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper/Orders/Order_Details/UnitPrice\": {\r\n" + 
    		"		\"type\": \"Decimal\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Product/Order_Details/UnitPrice\": {\r\n" + 
    		"		\"type\": \"Decimal\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/Orders/Employee/EmployeeID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Order/OrderDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper/Orders/Employee/EmployeeID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Orders/OrderDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/OrderID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/Employees1\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Employee\",\r\n" + 
    		"		\"numChildren\": 18,\r\n" + 
    		"		\"isArray\": true\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Order/Employee/EmployeeID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Order/Employee/BirthDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/Orders/ShippedDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Orders/RequiredDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Order/Order_Details/OrderID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/Territories/TerritoryID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"String\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/Employee1/BirthDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper/Orders/Order_Details/ProductID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Orders/ShippedDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/EmployeeID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Product/Order_Details/ProductID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/Orders/ShippedDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/Orders/Customer\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Customer\",\r\n" + 
    		"		\"numChildren\": 11\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Territories/Employees/HireDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper/Orders/Employee/Photo\": {\r\n" + 
    		"		\"type\": \"Binary\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/Orders/OrderID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper/Orders\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Order\",\r\n" + 
    		"		\"numChildren\": 14,\r\n" + 
    		"		\"isArray\": true\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Order/Shipper\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Shipper\",\r\n" + 
    		"		\"numChildren\": 3\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/BirthDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper/Orders/Order_Details\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Order_Detail\",\r\n" + 
    		"		\"numChildren\": 5,\r\n" + 
    		"		\"isArray\": true\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/Orders/OrderID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Product/UnitPrice\": {\r\n" + 
    		"		\"type\": \"Decimal\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/Employee1/HireDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Product\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Product\",\r\n" + 
    		"		\"numChildren\": 10\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/Employees1/EmployeeID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/Employee1/Photo\": {\r\n" + 
    		"		\"type\": \"Binary\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/Orders/ShippedDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/BirthDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/Orders/RequiredDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Shipper/Orders/Order_Details/OrderID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Orders/OrderID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Order\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Order\",\r\n" + 
    		"		\"numChildren\": 14\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/Orders\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Order\",\r\n" + 
    		"		\"numChildren\": 14,\r\n" + 
    		"		\"isArray\": true\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Discount\": {\r\n" + 
    		"		\"type\": \"Single\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Product/Category\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Category\",\r\n" + 
    		"		\"numChildren\": 4\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/Orders/Employee/HireDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/CustomerID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"String\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/Orders/Freight\": {\r\n" + 
    		"		\"type\": \"Decimal\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Orders/Order_Details/UnitPrice\": {\r\n" + 
    		"		\"type\": \"Decimal\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Order_Detail\",\r\n" + 
    		"		\"numChildren\": 5,\r\n" + 
    		"		\"isArray\": true\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Territories/Employees/Photo\": {\r\n" + 
    		"		\"type\": \"Binary\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Product/Order_Details/OrderID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/Orders/Order_Details/OrderID\": {\r\n" + 
    		"		\"isKey\": true,\r\n" + 
    		"		\"type\": \"Int32\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/Employees1/BirthDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Customer/Orders/Order_Details\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Order_Detail\",\r\n" + 
    		"		\"numChildren\": 5,\r\n" + 
    		"		\"isArray\": true\r\n" + 
    		"	},\r\n" + 
    		"	\"/Order_Details/Order/Employee\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Employee\",\r\n" + 
    		"		\"numChildren\": 18\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Territories\": {\r\n" + 
    		"		\"odata.type\": \"NorthwindModel.Territory\",\r\n" + 
    		"		\"numChildren\": 3,\r\n" + 
    		"		\"isArray\": true\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/Orders/RequiredDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/HireDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employees1/Employees1/BirthDate\": {\r\n" + 
    		"		\"type\": \"DateTime\"\r\n" + 
    		"	},\r\n" + 
    		"	\"/Employee/Employee1/Photo\": {\r\n" + 
    		"		\"type\": \"Binary\"\r\n" + 
    		"	}\r\n" + 
    		"}}";
    
//    @Test   
    @Order(1)
    public void testCreateProductOperation() throws Exception
    {
        ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);

        Map<String, Object> connProps = new HashMap<String,Object>();
        connProps.put(ODataClientConnection.ConnectionProperties.URL.toString(), _baseURL);
        
        Map<String, Object> opProps = new HashMap<String,Object>();
        opProps.put(ODataClientQueryOperation.OperationProperties.SERVICEPATH.toString(), WRITABLE_SERVICE_URL);

		HashMap<ObjectDefinitionRole, String> cookieMap = new HashMap<ObjectDefinitionRole, String>();
		cookieMap.put(ObjectDefinitionRole.INPUT, outputCookie);
		cookieMap.put(ObjectDefinitionRole.OUTPUT, outputCookie);
		
//		String payload = "{\"OrderID\":10248,\"CustomerID\":\"VINET\",\"EmployeeID\":5,\"OrderDate\":\"1996-07-04T00:00:00\",\"RequiredDate\":\"1996-08-01T00:00:00\",\"ShippedDate\":\"1996-07-16T00:00:00\",\"ShipVia\":3,\"Freight\":\"32.3800\",\"ShipName\":\"Vins et alcools Chevalier\",\"ShipAddress\":\"59 rue de l'Abbaye\",\"ShipCity\":\"Reims\",\"ShipRegion\":null,\"ShipPostalCode\":\"51100\",\"ShipCountry\":\"France\"}";
		String payload = "{\"ID\": 0, \"Name\": \"Bread\", \"Description\": \"Whole grain bread\", \"ReleaseDate\": \"2019-10-14T08:15:53.115+0200\", \"DiscontinuedDate\": null, \"Rating\": 4, \"Price\": \"2.5\"}";
		MockOperationContext context = new MockOperationContext(null, connector, OperationType.EXECUTE, connProps, opProps, objectTypeId, cookieMap, null);
        context.setCustomOperationType("POST");
        tester.setOperationContext(context);
        List<InputStream> inputs = new ArrayList<InputStream>();
        inputs.add(new ByteArrayInputStream(payload.getBytes()));
        List <SimpleOperationResult> actual = tester.executeExecuteOperation(inputs);
        System.out.println(actual.get(0).getStatusCode());
        String responseString = new String(actual.get(0).getPayloads().get(0));
        System.out.println(responseString);
        assertEquals("Created", actual.get(0).getMessage());
        assertEquals("201",actual.get(0).getStatusCode());
        assertEquals(1, actual.get(0).getPayloads().size());
//        _productID = responseObject.getLong("ID");
    }    
    
    @Test   
    @Order(2)
    public void testGetProductOperation() throws Exception
    {
    	ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);

        Map<String, Object> connProps = new HashMap<String,Object>();
        connProps.put(ODataClientConnection.ConnectionProperties.URL.toString(), _baseURL);
        
        Map<String, Object> opProps = new HashMap<String,Object>();
        opProps.put(ODataClientQueryOperation.OperationProperties.SERVICEPATH.toString(), SERVICE_URL);

		HashMap<ObjectDefinitionRole, String> cookieMap = new HashMap<ObjectDefinitionRole, String>();
		cookieMap.put(ObjectDefinitionRole.INPUT, inputCookie);
		cookieMap.put(ObjectDefinitionRole.OUTPUT, outputCookie);
		MockOperationContext context = new MockOperationContext(null, connector, OperationType.EXECUTE, connProps, opProps, objectTypeId, cookieMap, null);
        context.setCustomOperationType("GET");
	
		tester.setOperationContext(context);
        List<InputStream> inputs = new ArrayList<InputStream>();
        inputs.add(new ByteArrayInputStream(jsonPayload.getBytes()));
        List <SimpleOperationResult> actual = tester.executeExecuteOperation(inputs);
        assertEquals("OK", actual.get(0).getMessage());
        assertEquals("200",actual.get(0).getStatusCode());
        assertEquals(1, actual.get(0).getPayloads().size());
        String responseString = new String(actual.get(0).getPayloads().get(0));
        System.out.println(responseString);
    }

//    @Test 
    @Order(3)
    public void testDeleteProductOperation() throws Exception
    {
    	ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);

        Map<String, Object> connProps = new HashMap<String,Object>();
        connProps.put(ODataClientConnection.ConnectionProperties.URL.toString(), _baseURL);
        
        Map<String, Object> opProps = new HashMap<String,Object>();
        opProps.put(ODataClientQueryOperation.OperationProperties.SERVICEPATH.toString(), SERVICE_URL);

		HashMap<ObjectDefinitionRole, String> cookieMap = new HashMap<ObjectDefinitionRole, String>();
		cookieMap.put(ObjectDefinitionRole.INPUT, inputCookie);
		MockOperationContext context = new MockOperationContext(null, connector, OperationType.EXECUTE, connProps, opProps, objectTypeId, cookieMap, null);
        context.setCustomOperationType("DELETE");
	
		tester.setOperationContext(context);
        List<InputStream> inputs = new ArrayList<InputStream>();
        inputs.add(new ByteArrayInputStream(jsonPayload.getBytes()));
        List <SimpleOperationResult> actual = tester.executeExecuteOperation(inputs);
        assertEquals("Forbidden", actual.get(0).getMessage());
        assertEquals("403",actual.get(0).getStatusCode());
        assertEquals(1, actual.get(0).getPayloads().size());
        String responseString = new String(actual.get(0).getPayloads().get(0));
        System.out.println(responseString);
    }
    
//    @Test   
    public void testQueryOperation() throws Exception
    {
    	ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);

        Map<String, Object> connProps = new HashMap<String,Object>();
        connProps.put(ODataClientConnection.ConnectionProperties.URL.toString(), _baseURL);
        
        Map<String, Object> opProps = new HashMap<String,Object>();
        opProps.put(ODataClientQueryOperation.OperationProperties.SERVICEPATH.toString(), SERVICE_URL);
        opProps.put(ODataClientQueryOperation.OperationProperties.PAGESIZE.name(), 40L);
//        opProps.put("PAGEITEMPATH", "/value/*");
        opProps.put(ODataClientQueryOperation.OperationProperties.MAXDOCUMENTS.name(), 187L);

        QueryFilter qf = new QueryFilter();
        
		HashMap<ObjectDefinitionRole, String> cookieMap = new HashMap<ObjectDefinitionRole, String>();
		cookieMap.put(ObjectDefinitionRole.OUTPUT, ordersCookie);
		List<String> selectedFields = new ArrayList<String>();
		selectedFields.add("OrderID");
		selectedFields.add("Order_Details/Product/Category/CategoryName");
		selectedFields.add("Order_Details/Quantity");
		selectedFields.add("Order_Details/Product/ProductName");
		MockOperationContext context = new MockOperationContext(null, connector, OperationType.QUERY, connProps, opProps, "Orders", cookieMap, selectedFields);
	
		tester.setOperationContext(context);
        List <SimpleOperationResult> actual = tester.executeQueryOperation(qf);
        String responseString = new String(actual.get(0).getPayloads().get(0));
        System.out.println(responseString);
        assertEquals("OK", actual.get(0).getMessage());
        assertEquals("200",actual.get(0).getStatusCode());
        assertEquals(187, actual.get(0).getPayloads().size());
    }
}
