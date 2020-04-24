# Azure Cosmos DB Live Data Migrator
Simple and reliable service to transfer data from one Cosmos DB SQL API container to another in a live fashion.

## Features

The Cosmos DB Live Data Migrator provides the following features:

* Live data migration from a source Cosmos DB Container to a target Cosmos DB Container
* Ability to read and migrate from a given point of time in source
* Ability to scale up or scale down the number of workers while the migration is in progress
* Support for dead-letter queue where in the Failed import documents and the Bad Input documents are stored in the configured Azure storage location so as to help perform point inserts post migration
* Migration Monitoring web UI that tracks the completion percentage, ETA, Average insert rate etc:


## Getting Started

### Under the hood
- Uses change feed processor to read from the source container
- Uses bulk executor to write to the target container
- Uses ARM template to deploy the resources
- Uses App Service compute with P3v2 SKU in PremiumV2 Tier. The default number of instances is 5 and can be scaled up or down. 


### Quickstart

Click deploy to Azure: 
<br/><br/>


<a href="https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2FAzure-Samples%2Fazure-cosmosdb-live-data-migrator%2Fmaster%2FMigrationAppResourceGroup%2Fazuredeploy.json" target="_blank">
    <img src="https://raw.githubusercontent.com/Azure/azure-quickstart-templates/master/1-CONTRIBUTION-GUIDE/images/deploytoazure.svg?sanitize=true"/></a>

<br/>

You will then be presented will some fields you need to populate for the deployment.

- It may be a good practice to create a new Resource group so that it is easy to co-locate the different components of Service. Please make sure that the Resource group Region is same as the Region of Cosmos DB Source and Target collections.

- Provide an identifiable Appname and an Appinsights name

- The cosmos db Account information is to store the migration metadata and migration state. Please note that this is not the Source and Target Cosmos DB details and that will need to be entered at a later stage.

- The Clientpackagelocation and Migrationjoblocation are pre-populated with the zipped files to be published

	![Templateparams](images/customdeployment.png)

- Open the webapp client resource and click on the URL (it will be of the format: https://appnameclient.azurewebsites.net)
	![Webappclient](images/webappclient.png)

- Add the Source and Target Cosmos DB connection details 

- Add the Cosmos DB connection details for Lease DB, which is used in the ChangeFeed process. 

- [Optional] Add the Azure Blob Connection string and Container Name to store the failed / bad records. The complete records would be stored in this Container and can be used for point inserts.

- [Optional] Maximum data age in hours is used to derive the starting point of time to read from source container. In other words, it starts looking for changes after [currenttime - given number of hours] in source. The data migration starts from beginning if this parameter is not specified.

- [Optional] The "Source Partition Key Attributes" and "Target Partition Key Attribute" fields are used for mapping partition key attributes from your source collection to your target collection. For example, if you want to have a dedicated or synthetic partition key in your new collection named "partitionKey", and this will be populated from "deviceId", you should enter `deviceId` in "Source Partition Key Attributes" field, and `partitionKey` in the "Target Partition Key" field. You can also add multiple fields separated by a comma to map a synthetic key, e.g. add `deviceId,timestamp` in the "Source Partition Key Attribute(s)" field. Nested source fields are also supported, for example `Parent1/Child1,Parent2/Child2`. If you want to select an item in an array, for example:

	```json
		{
			"id": "1",
			"parent": [
				{
					"child": "value1"
				},
				{
					"child": "value2"
				}
			]
		}
	```
	You can use xpath syntax, e.g. `parent/item[1]/child`. In all cases of synthetic partition key mapping, these will be separated with a dash when mapped to the target collection, e.g. `value1-value2`. If no mapping is required, as there is no dedicated partition key field in your source or target collection, you can leave these fields blank.
 
![Migrationdetails](images/migrationdetails.png)

- Click on Start MIgration and it will automatically be taken to the Monitoring web UI. It provides the data migration stats and metrics such as counts, completion percentage, ETA and Average insert rate as seen below. 

	![Monitoring](images/monitoring.png)

- Click on Complete Migration once all the documents have been migrated to Target container. If you are doing a live migration for the purpose of changing partition key, you should not complete the migration until you have made any required changes in your client code based on the new partition key scheme. 

- The number of workers in the webapp service can be scaled up or down while the migration is in progress as shown below. The default is five workers.

	![Scaling](images/scaling.png)

- The Application Insights tracks additional migration metrics such as failed records count, bad record count and RU consumption and can be queried. 
	


### Failed / Bad Documents
- TBA 


### Querying App Insights metrics
- TBA



