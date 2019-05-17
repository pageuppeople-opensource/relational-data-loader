IF NOT EXISTS (SELECT * FROM sys.databases WHERE Name = 'RDL_Integration_Test_Source_Db')
	CREATE DATABASE	 RDL_Integration_Test_Source_Db

ALTER DATABASE RDL_Integration_Test_Source_Db
SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
