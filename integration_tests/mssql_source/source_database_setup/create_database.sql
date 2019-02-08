IF NOT EXISTS (SELECT * FROM sys.databases WHERE Name = 'RelationalDataLoaderIntegrationTestSource')
	CREATE DATABASE	 RelationalDataLoaderIntegrationTestSource

ALTER DATABASE RelationalDataLoaderIntegrationTestSource
SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)
