# blobrepl

There are not enough Python examples out there to do stuff on Azure. So here is one fairly simple class which can be used to replicate a blobs from one storage account to another storage account.

This type of copy, can be hooked up to a Storage Event using Azure Functions. Then if an add or delete is triggered on a primary account, you can replicate into a second storage account.


The settings.json file is where you add your storage account connection strings. 

## Be very careful with the storage account connection strings, don't upload your string to the www

#Todo
Error Checking
Turn into Event Driven Fuction - ?? Possibly

20200809 - Initial Basic Version.