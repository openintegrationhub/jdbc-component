# JDBC-component
## Description
This is an open source component for working with object-relational database management systems on [elastic.io platform](http://www.elastic.io "elastic.io platform").
### Purpose
With this component you will have following triggers:

SELECT - this trigger will execute an [SQL](https://en.wikipedia.org/wiki/SQL "SQL") query that returns multiple results, it has limitations on the query and suited only for SELECT type of queries. The trigger will remember last execution timestamp and let you build queries on it.

GET ROWS POLLING - this trigger will execute select query from specified table with simple criteria of selected datetime or timestamp table. The trigger will remember last execution timestamp and let you build queries on it.

Following actions are inside:

SELECT - this action will execute an [SQL](https://en.wikipedia.org/wiki/SQL "SQL") query that returns multiple results, it has limitations on the query and suited only for SELECT type of queries.

LOOKUP BY PRIMARY KEY - this action will execute select query from specified table, as criteria can be used only [PRIMARY KEY](https://en.wikipedia.org/wiki/Primary_key "PRIMARY KEY"). The action returns only one result (a primary key is unique). 

DELETE BY PRIMARY KEY - this action will execute delete query from specified table, as criteria can be used only [PRIMARY KEY](https://en.wikipedia.org/wiki/Primary_key "PRIMARY KEY"). The action returns an integer value that indicates the number of rows affected, the returned value can be 0 or 1 (a primary key is unique).
### How works
### Requirements
Before you can deploy any code into elastic.io **you must be a registered elastic.io platform user**. Please see our home page at [http://www.elastic.io](http://www.elastic.io) to learn how. 
#### Environment variables
For unit-testing
#### Others
## Credentials
### field1
### field2
## Actions
### Action1
<Brief description>
#### Input fields description (if there is any)
#### Input json schema location
#### Output json schema location (if exists)
### Action2
<Brief description>
#### Input fields description (if there is any)
#### Input json schema location
#### Output json schema location (if exists)
## Triggers (if any)
### Trigger1
### Trigger2
## Additional info (if any)
## <System> API and Documentation links
