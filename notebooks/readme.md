# Helpful notebooks that make administrating your delta lakehouse simpler #


## Delta Lakehouse Data Profiler ##

Notebook: Create Data Profile.dbc

<img src="https://i.imgur.com/NNyw4Md.png" width="20%">

Use this notebook to create a schema table and create a data profile table 

Select the database in the widget at the top, and the click run all

This notebook will create 2 tables in the target database: 

### Data Profile ###

` profileData (databaseName string, tableName string, columnName  string, dataType string, value string, num_records float, len float) `

Basic profile information for all tables in the database, exclusion list provided



### Schema Information ###

` SchemaInformation (databaseName string, tableName string, columnName string, dataType string, comments string) `

Basic Schema information that you can query with SQL, can be used to create dynamic SQL for common data processing

