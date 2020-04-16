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

- Sign in to the Azure portal and select Create a resource option
- Find Template deployment (deploy using custom templates) and choose Buld your own template as below. 
	
	![Templatedeployment](images/templatedeployment.png)
	![Templatedeploymentcreate](images/templatedeploymentcreate.png)
	![Templatedeploymentbuild](images/templatedeploymentbuild.png)

- Copy the contents of [deployment template](MigrationAppResourceGroup/azuredeploy.json) and click Save.

- It may be a good practice to create a new Resource group so that it is easy to co-locate the different components of Service. Please make sure that the Resource group Region is same as the Region of Cosmos DB Source and Target collections.

- Provide an identifiable Appname and an Appinsights name

- The cosmos db Account information is to store the migration metadata and migration state. Please note that this is not the Source and Target Cosmos DB details and that will need to be entered at a later stage.

- The Clientpackagelocation and Migrationjoblocation are pre-populated with the zipped files to be published
	![Templateparams](images/templateparams.png)

- Open the webapp client resource and click on the URL (it will be of the format: https://appnameclient.azurewebsites.net)
	![Webappclient](images/webappclient.png)

- Add the Source and Target Cosmos DB connection details 

- Add the Cosmos DB connection details for Lease DB, which is used in the ChangeFeed process. 

- [Optional] Add the Azure Blob Connection string and Container Name to store the failed / bad records. The complete records would be stored in this Container and can be used for point inserts.

- [Optional] Maximum data age in hours is used to derive the starting point of time to read from source container. In other words, it starts looking for changes after [currenttime - given number of hours] in source. The data migration starts from beginning if this parameter is not specified.

- [Optional] The "Source Partition Key Attribute(s)" and "Target Partition Key" fields are used for mapping a dedicated or synthetic partition key attribute in your target collection. For example, if you want to have a dedicated or synthetic partition key in your new collection named "partitionKey", and this will be populated from "deviceId", you should enter "deviceId" in "Source Partition Key Attribute(s)" field, and "partitionKey" in the "Target Partition Key" field. You can also add multiple fields separated by a comma to map a synthetic key, e.g. add "deviceId,timestamp" in the "Source Partition Key Attribute(s)" field. These will be separated with a dash, e.g. "deviceId-timestamp". Mapping to, or from, nested fields is not supported. If no mapping is required, as there is no dedicated partition key field in your target collection, you can leave these fields blank.
 
![Migrationdetails](images/migrationdetails.png)

- Click on Start MIgration and it will automatically be taken to the Monitoring web UI. It provides the data migration stats and metrics such as counts, completion percentage, ETA and Average insert rate as seen below. 

	![Monitoring](images/monitoring.png)

- Click on Complete Migration once all the documents have been migrated to Target container

- The number of workers in the webapp service can be scaled up or down while the migration is in progress as shown below. The default is five workers.

	![Scaling](images/scaling.png)

- The Application Insights tracks additional migration metrics such as failed records count, bad record count and RU consumption and can be queried. 
	


### Failed / Bad Documents
- TBA 


### Querying App Insights metrics
- TBA



