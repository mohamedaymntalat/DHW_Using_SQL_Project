/*
This code create a datawarehouse and schemas
you must check if the database is already exist or not
*/

--This create a database
CREATE DATABASE Datawarehouse;

--This USING a Datawarehouse database
use Datawarehouse;

GO  --let sql to excute each line sperately
--WE create THREE  SCHEMA 

CREATE SCHEMA bronze ;
Go
CREATE SCHEMA silver ;
GO
CREATE SCHEMA gold ;

