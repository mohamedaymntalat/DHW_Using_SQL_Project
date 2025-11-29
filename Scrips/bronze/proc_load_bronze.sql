/*
Load The Data From 2 sources To Table
Calculate the Duration time of loading whole datat and each table

To Use Excute this -->  EXEC bronze.load_bronze
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	
	DECLARE @start_time DATETIME , @end_time DATETIME ,@start_batch_time DATETIME , @end_batch_time DATETIME
	BEGIN TRY
	SET @start_batch_time = GETDATE();
		PRINT'Loding Bronze Layer'
		PRINT'======================='
		PRINT'Crm Tables'
		print '======================'

		SET @start_time = GETDATE();
		print'Truncate and Insert crm_cust_info '
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\datasets\source_crm\cust_info.csv'
		WITH (
			FirstRow = 2 ,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds'
		print '======================'


		SET @start_time= GETDATE();
		print'Truncate and Insert crm_prd_info '
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\datasets\source_crm\prd_info.csv'
		WITH (
			FirstRow = 2 ,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT 'Load Duration:'+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds'
		print '======================'

		SET @start_time = GETDATE();
		print'Truncate and Insert crm_sales_details '
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration:'+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds'

		print '======================'
		PRINT'Erp Tables'
		print '======================'
		SET @start_time = GETDATE();
		print'Truncate and Insert erp_cust_az12 '
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration:'+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds'

		print '======================'
		SET @start_time = GETDATE();
		print'Truncate and Insert erp_loc_a101 '
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration:'+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds'
		print '======================'
		SET @start_time = GETDATE();
		print'Truncate and Insert erp_px_cat_g1v2 '
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\datasets\source_erp\px_cat_g1v2.csv'
		WITH(
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT 'Load Duration:'+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds'
		print '======================'
	END TRY
	BEGIN CATCH
	PRINT '========='
	PRINT 'ERROR MESSAGE'+ ERROR_MESSAGE()
	PRINT 'ERROR NUMBER'+ CAST (ERROR_NUMBER() AS NVARCHAR)
	END CATCH
	SET @end_batch_time = GETDATE()
	PRINT 'ALL Load Duration : ' + CAST(DATEDIFF(SECOND,@start_batch_time,@end_batch_time) AS NVARCHAR)+' seconds'
END
