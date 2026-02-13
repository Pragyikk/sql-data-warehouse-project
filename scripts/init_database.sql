/*
==========================================================
Create Database and Schemas
==========================================================

This script create a new database named DataWarehousing.
If it already exists then it is dropped and recreated.
The script sets up three schemas 'bronze', 'silver', 'gold'

WARNING:
	Running this script will drop the entire DataWarehouse if it exists.
*/
USE master;
GO

--drop and recreate the DataWarehouse database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name= 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse
END;
GO

--create the DataWarehouse database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse
GO

--create schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
