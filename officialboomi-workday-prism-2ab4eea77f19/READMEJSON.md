## Workday Prism Analytics V2 connector -Input and Output Json ##

This file contains the input and output JSON profiles for all the operations in Workday Prism Analytics V2 connector.

### Create Table: ###

  {
    "name": "Dell_Boomi_Table_100_27_007",
    "displayName": "Table Created on 27th March 2020_007",
    "fields": [
        {
            "ordinal": 1,
            "name": "Employee",
            "description": "Employee",
            "precision": 255,
            "scale": 0,
            "type": {
                "id": "fdd7dd26156610006a12d4fd1ea300ce",
                "descriptor": "Text"
            }
        },
        {
            "ordinal": 2,
            "name": "Employee_ID",
            "description": "Employee ID",
            "precision": 255,
            "scale": 0,
            "type": {
                "id": "fdd7dd26156610006a12d4fd1ea300ce",
                "descriptor": "Text"
            }
        },
        {
            "ordinal": 3,
            "name": "Job_Title",
            "description": "Job Title",
            "precision": 255,
            "scale": 0,
            "type": {
                "id": "fdd7dd26156610006a12d4fd1ea300ce",
                "descriptor": "Text"
            }
        },
        {
            "ordinal": 4,
            "name": "Supervisory_Organization",
            "description": "Supervisory Organization",
            "precision": 255,
            "scale": 0,
            "type": {
                "id": "fdd7dd26156610006a12d4fd1ea300ce",
                "descriptor": "Text"
            }
        },
        {
            "ordinal": 5,
            "name": "Supervisory_Organization_ID",
            "description": "Supervisory Organization - ID",
            "precision": 255,
            "scale": 0,
            "type": {
                "id": "fdd7dd26156610006a12d4fd1ea300ce",
                "descriptor": "Text"
            }
        },
        {
            "ordinal": 6,
            "name": "Manager",
            "description": "Manager",
            "precision": 255,
            "scale": 0,
            "type": {
                "id": "fdd7dd26156610006a12d4fd1ea300ce",
                "descriptor": "Text"
            }
        },
        {
            "ordinal": 7,
            "name": "Business_Site",
            "description": "Business Site",
            "precision": 255,
            "scale": 0,
            "type": {
                "id": "fdd7dd26156610006a12d4fd1ea300ce",
                "descriptor": "Text"
            }
        },
        {
            "ordinal": 8,
            "name": "Annual_Salary",
            "description": "Annual Salary",
            "precision": 18,
            "scale": 2,
            "type": {
                "id": "32e3fa0dd9ea1000072bac410415127a",
                "descriptor": "Numeric"
            }
        },
        {
            "ordinal": 9,
            "name": "Salary_Currency",
            "description": "Salary Currency",
            "precision": 255,
            "scale": 0,
            "type": {
                "id": "fdd7dd26156610006a12d4fd1ea300ce",
                "descriptor": "Text"
            }
        },
        {
            "ordinal": 10,
            "name": "Last_Performance_Rating",
            "description": "Last Performance Rating",
            "precision": 255,
            "scale": 0,
            "type": {
                "id": "fdd7dd26156610006a12d4fd1ea300ce",
                "descriptor": "Text"
            }
        }
    ]
}

### Create Bucket: ###

{
    "name": "Bucket_for_table_'{1}'",
    "targetDataset": {
        "id": "'{1}'"
    },
    "operation": {
        "id": "Operation_Type=Replace"
    },
    "schema": {
        "parseOptions": {
            "fieldsDelimitedBy": ",",
            "fieldsEnclosedBy": "\"",
            "headerLinesToIgnore": 1,
            "charset": {
                "id": "Encoding=UTF-8"
            },
            "type": {
                "id": "Schema_File_Type=Delimited"
            }
        },
        "fields": [
            {
                "ordinal": 1,
                "name": "Employee",
                "description": "Employee",
                "precision": 255,
                "scale": 0,
                "type": {
                    "id": "fdd7dd26156610006a12d4fd1ea300ce",
                    "descriptor": "Text"
                }
            },
            {
                "ordinal": 2,
                "name": "Employee_ID",
                "description": "Employee ID",
                "precision": 255,
                "scale": 0,
                "type": {
                    "id": "fdd7dd26156610006a12d4fd1ea300ce",
                    "descriptor": "Text"
                }
            },
            {
                "ordinal": 3,
                "name": "Job_Title",
                "description": "Job Title",
                "precision": 255,
                "scale": 0,
                "type": {
                    "id": "fdd7dd26156610006a12d4fd1ea300ce",
                    "descriptor": "Text"
                }
            },
            {
                "ordinal": 4,
                "name": "Supervisory_Organization",
                "description": "Supervisory Organization",
                "precision": 255,
                "scale": 0,
                "type": {
                    "id": "fdd7dd26156610006a12d4fd1ea300ce",
                    "descriptor": "Text"
                }
            },
            {
                "ordinal": 5,
                "name": "Supervisory_Organization_ID",
                "description": "Supervisory Organization - ID",
                "precision": 255,
                "scale": 0,
                "type": {
                    "id": "fdd7dd26156610006a12d4fd1ea300ce",
                    "descriptor": "Text"
                }
            },
            {
                "ordinal": 6,
                "name": "Manager",
                "description": "Manager",
                "precision": 255,
                "scale": 0,
                "type": {
                    "id": "fdd7dd26156610006a12d4fd1ea300ce",
                    "descriptor": "Text"
                }
            },
            {
                "ordinal": 7,
                "name": "Business_Site",
                "description": "Business Site",
                "precision": 255,
                "scale": 0,
                "type": {
                    "id": "fdd7dd26156610006a12d4fd1ea300ce",
                    "descriptor": "Text"
                }
            },
            {
                "ordinal": 8,
                "name": "Annual_Salary",
                "description": "Annual Salary",
                "precision": 18,
                "scale": 2,
                "type": {
                    "id": "32e3fa0dd9ea1000072bac410415127a",
                    "descriptor": "Numeric"
                }
            },
            {
                "ordinal": 9,
                "name": "Salary_Currency",
                "description": "Salary Currency",
                "precision": 255,
                "scale": 0,
                "type": {
                    "id": "fdd7dd26156610006a12d4fd1ea300ce",
                    "descriptor": "Text"
                }
            },
            {
                "ordinal": 10,
                "name": "Last_Performance_Rating",
                "description": "Last Performance Rating",
                "precision": 255,
                "scale": 0,
                "type": {
                    "id": "fdd7dd26156610006a12d4fd1ea300ce",
                    "descriptor": "Text"
                }
            }
        ],
        "schemaVersion": {
            "id": "Schema_Version=1.0"
        }
    }
}

#### JSON format to create table ####

{
    "name": "name of the table",
    "displayName": "display name of the table",
    "fields": [
       {
            "ordinal": 1,
            "name": "Column Name",
            "description": "Description of the column",
            "precision": 255,
            "scale": 0,
            "type":{
                "id": "fdd7dd26156610006a12d4fd1ea300ce",
                "descriptor": "Text"
            	  }
          },
       {
            "ordinal": 2,
            "name": "Column Name",
            "description": "Description of the column",
            "precision": 255,
            "scale": 0,
            "type":{
                "id": "fdd7dd26156610006a12d4fd1ea300ce",
                "descriptor": "Text"
            	  }
          },
   ]
}

#### JSON format to create Bucket ####

{
"name": "BucketForDYNAMIC_DATASET_'{1}'",
"targetDataset": {      
"id": "b818f1f5fbf0017544cf2700c0393b15"
},
"operation": {
"id": "Operation_Type=Replace"
},
"schema": {
"parseOptions": {
"fieldsDelimitedBy": ",",
"fieldsEnclosedBy": "\"",
"headerLinesToIgnore": 1,
"charset": {
"id": "Encoding=UTF-8"        
},
"type": {
    "id": "Schema_File_Type=Delimited"
     }
     },
     "fields": [
			{
              "ordinal": 1,
              "name": "Employee",
              "description": "Employee",
              "precision": 255,
              "scale": 0,
              "type": {
                  "id": "fdd7dd26156610006a12d4fd1ea300ce",
                  "descriptor": "Text"
					}
			},
			{
            "ordinal": 8,
            "name": "Annual_Salary",
            "description": "Annual Salary",
            "precision": 18,
            "scale": 2,
            "type": {	
                "id": "32e3fa0dd9ea1000072bac410415127a",
                "descriptor": "Numeric"
                }
			},
                ],
                "schemaVersion": {
                    "id": "Schema_Version=1.0"   }
         }
}