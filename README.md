# Azure Data Factory templates

This repository contains Azure Data Factory templates that you can import into your own projects. Create a data factory in Azure and enter its name and resource group in the below template deployment.

<a href="https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2FrealAngryAnalytics%2Fadf%2Fmaster%2Fazuredeploy.json" target="_blank">
    <img src="http://azuredeploy.net/deploybutton.png"/>
</a>

This is a template that requires three connection strings currently
* blob storage account connection string - will be used for staging the load to SQL DW. This can be found in your "Keys" dialog of your blob storage account
* Source azure sql database connection string - will be the source. Found in "settings -> Connection Strings" dialog. Using SQL Authentication is probably easiest. Replace "User ID" and "Password" entries with your real values or the deployment will fail.
    * Even if you choose to use Teradata or Netezza or SQL on prem for the source, go ahead and use an azure sql database so that the template deploys, then you can modify the source. (it will cost you $5 a month)
* Destination azure sql data warehouse connection string - will be the destination. Just like Azure SQL DB, it is found under "settings -> Connection Strings" dialog. Don't forget to change your User ID and Password entries




There is a sqlscripts folder that contains supporting sql scripts for control tables, stored procedures, supporting data creation for the ADF pipelines available here.

## Delta load with updates
There is a supporting blog post for this here: http://angryanalytics.com 

There are source, destination, and data generation scripts here: https://github.com/realAngryAnalytics/adf/tree/master/sqlscripts/delta_load_w_updates 

The database is based on AdventureWorks DW 2016. CTP3 version to be exact. It can be downloaded here: https://www.microsoft.com/en-us/download/details.aspx?id=49502

I am using Azure SQL Database for the source. If you choose to use an on prem SQL for this example you will need an integration runtime and modification to the Linked Connections in the ADF template.




