# JDBC-component
## Description
This is an open source component for working with object-relational database management systems on [elastic.io platform](http://www.elastic.io "elastic.io platform").
### Purpose
With this component you will have following triggers:

``SELECT`` - this trigger will execute an [SQL](https://en.wikipedia.org/wiki/SQL "SQL") query that returns multiple results, it has limitations on the query and suited only for SELECT type of queries. The trigger will remember last execution timestamp and let you build queries on it.

``GET ROWS POLLING`` - this trigger will execute select query from specified table with simple criteria of selected datetime or timestamp table. The trigger will remember last execution timestamp and let you build queries on it.

Following actions are inside:

``SELECT`` - this action will execute an [SQL](https://en.wikipedia.org/wiki/SQL "SQL") query that returns multiple results, it has limitations on the query and suited only for SELECT type of queries.

``LOOKUP BY PRIMARY KEY`` - this action will execute select query from specified table, as criteria can be used only [PRIMARY KEY](https://en.wikipedia.org/wiki/Primary_key "PRIMARY KEY"). The action returns only one result (a primary key is unique). 

``DELETE BY PRIMARY KEY`` - this action will execute delete query from specified table, as criteria can be used only [PRIMARY KEY](https://en.wikipedia.org/wiki/Primary_key "PRIMARY KEY"). The action returns an integer value that indicates the number of rows affected, the returned value can be 0 or 1 (a primary key is unique).
### How works

### Requirements
Before you can deploy any code into elastic.io **you must be a registered elastic.io platform user**. Please see our home page at [http://www.elastic.io](http://www.elastic.io) to learn how. 
#### Environment variables
For unit-testing
#### Others
## Credentials
You may use following properties to configure a connection:
![image](https://user-images.githubusercontent.com/40201204/43577550-ce99efe6-9654-11e8-87ed-f3e0839d618a.png)
You can add the authorisation methods during the integration flow design or by going to your Settings > Security credentials > REST client and adding there.
### DB Engine
![image](https://user-images.githubusercontent.com/40201204/43577772-6f85bdea-9655-11e8-96e1-368493a36c9d.png)
You are able to choose one of existing database types
- ``MySQL`` - compatible with MySQL Server 5.5, 5.6, 5.7 and 8.0.
- ``PostgreSQL`` - compatible with PostgreSQL 8.2 and higher
- ``Oracle`` - compatible with Oracle Database 8.1.7 - 12.1.0.2
- ``MSSQL`` - compatible with Microsoft SQL Server 2008 R2 and higher
### Connection URI
In the Connection URI field please provide hostname of the server, e.g. ``acme.com``
### Connection port
In the Connection port field please provide port of the server instance, as by default:
- ``3306`` - MySQL
- ``5432`` - PostgreSQL
- ``1521`` - Oracle
- ``1433`` - MSSQL
### Database Name
In the Database Name field please provide name of database at the instance that you want to interact with.
### User
In the User field please provide a username that has permissions to interact with the Database.
### Password
In the Password field please provide a password of the user that has permissions to interact with the Database.

Validation will start right after click on a Save button. You will be able to continue working with component after validation if all provided credentials will be valid.
## Triggers
### SELECT
You are able to provide SELECT query with last execution timestamp as WHERE clause criteria.
![image](https://user-images.githubusercontent.com/40201204/43591075-2a032dcc-967b-11e8-968d-851355c2646e.png)
Before executing the statement %%EIO_LAST_POLL%% will be replaced with ISO Date of the last execution or max value of the last pooled datetime.
Precision of the polling clause can be till milliseconds.
The format of ``Start Polling From (optional)`` field should be like ``yyyy-mm-dd hh:mi:ss[.sss]``, where
- ``yyyy`` - year
- ``mm`` - month
- ``dd`` - day
- ``hh`` - hour
- ``mi`` - minute
- ``ss`` - second
- ``sss`` - millisecond (optional)
### GET ROWS POLLING
This trigger can polling data from provided table. As WHERE clause you can use column, which has datatype like DATE or TIMESTAMP.
![image](https://user-images.githubusercontent.com/40201204/43591332-c99f6b3e-967b-11e8-8a77-bf8386e83d51.png)
Before executing the clause for specified column will be replaced with ISO Date of the last execution or max value of the last pooled datetime.
Precision of the polling clause can be till milliseconds.
The format of cStart Polling From (optional)`` field should be like ``yyyy-mm-dd hh:mi:ss[.sss]``, where
- ``yyyy`` - year
- ``mm`` - month
- ``dd`` - day
- ``hh`` - hour
- ``mi`` - minute
- ``ss`` - second
- ``sss`` - millisecond (optional)
## Actions
### SELECT
![image](https://user-images.githubusercontent.com/40201204/43592439-39ec5738-967e-11e8-8632-3655b08982d3.png)
The action will build an [SQL](https://en.wikipedia.org/wiki/SQL "SQL") query that can return multiple results, it has limitations on the query and suited only for SELECT type of queries.
#### Input fields description

### LOOKUP BY PRIMARY KEY
![image](https://user-images.githubusercontent.com/40201204/43592505-5b6bbfe8-967e-11e8-845e-2ce8ac707357.png)
The action will execute select query from a table from a ``Table`` dropdown field, as criteria can be used only [PRIMARY KEY](https://en.wikipedia.org/wiki/Primary_key "PRIMARY KEY"). The action returns only one result (a primary key is unique)
#### Input fields description

### DELETE BY PRIMARY KEY
![image](https://user-images.githubusercontent.com/40201204/43592505-5b6bbfe8-967e-11e8-845e-2ce8ac707357.png)
#### Input fields description


<Brief description>
#### Input fields description (if there is any)
#### Input json schema location
#### Output json schema location (if exists)

<Brief description>
### Input fields description (if there is any)
### Input json schema location
### Output json schema location (if exists)
## Triggers (if any)
### Trigger1
### Trigger2
## Additional info (if any)
## <System> API and Documentation links
