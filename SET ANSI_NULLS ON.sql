SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Alter PROCEDURE [dbo].[usp_transform_customer_nashville_dim]
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = SYSUTCDATETIME();
    DECLARE @nashville_rows_inserted INT = 0;
    DECLARE @rows_inserted INT = 0;
    DECLARE @rows_total INT = 0;
    DECLARE @status_message NVARCHAR(4000) = '';
    DECLARE @status INT = 1; -- 1 = OK by default
    DECLARE @error_message NVARCHAR(4000) = '';
    DECLARE @exec_duration_seconds DECIMAL(10,2);

    BEGIN TRY
        -- Create table if it doesn't exist
        IF NOT EXISTS (
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = 'customer_nashville_dim' AND TABLE_SCHEMA = 'dbo'
        )
        BEGIN
            CREATE TABLE dbo.customer_nashville_dim (
                ar_city VARCHAR(100),
                ar_credit_limit DECIMAL(20,6),
                ar_cust_name VARCHAR(60),
                ar_date_setup DATETIME2(0),
                ar_master_id VARCHAR(60),
                ar_zip VARCHAR(15),
                sy_state_id VARCHAR(6),
                description VARCHAR(50),
                ar_sales_name VARCHAR(17),
                ar_csr_name VARCHAR(17),
                iso_country VARCHAR(2),
                name VARCHAR(100),
                syt_description VARCHAR(16),
                vertical VARCHAR(100),
                created_ts DATETIME2(0) NOT NULL,
                modified_ts DATETIME2(0) NOT NULL
            );
        END

        -- Full load
        TRUNCATE TABLE dbo.customer_nashville_dim;

        INSERT INTO dbo.customer_nashville_dim (
            ar_city, ar_credit_limit, ar_cust_name, ar_date_setup, ar_master_id, ar_zip, sy_state_id, description, ar_sales_name, ar_csr_name, iso_country, name, syt_description, vertical, created_ts, modified_ts
        )      
        SELECT
            cu.arcity as ar_city,
            cu.arcreditlimit as ar_credit_limit,
            cu.arcustname as ar_cust_name,
            cu.ardatesetup as ar_date_setup,
            cu.armasterid as ar_master_id,
            cu.arzip as ar_zip,
            cu.systateid as sy_state_id,
            cg.description as description,
            sp.arsalesname as ar_sales_name,
            csr.arcsrname as ar_csr_name,
            ct.isocountry as iso_country,
            ct.name as name,
            tm.sytdescription as syt_description,
            'Unassigned' as vertical,
            SYSUTCDATETIME(), -- created_ts
            SYSUTCDATETIME()  -- modified_ts
        FROM WH_Silver.nashville.epace_public_customer cu
        LEFT OUTER JOIN WH_Silver.nashville.epace_public_customergroup cg ON cu.customergroup = cg.id
        LEFT OUTER JOIN WH_Silver.nashville.epace_public_salesperson sp ON cu.arsalesid = sp.arsalesid
        LEFT OUTER JOIN WH_Silver.nashville.epace_public_csr csr ON cu.arcsrid = csr.arcsrid
        LEFT OUTER JOIN WH_Silver.pace.epace_public_country ct ON cu.sycountry = ct.id
        LEFT OUTER JOIN WH_Silver.nashville.epace_public_terms tm ON cu.sytermsid = tm.sytermsid
        LEFT OUTER JOIN WH_Silver.pace.epace_public_u_customer uc ON cu.armasterid = uc.armasterid;

        SET @nashville_rows_inserted = @@ROWCOUNT;
        SET @rows_inserted = @nashville_rows_inserted;

        SET @rows_total = @rows_inserted;
        SET @exec_duration_seconds = DATEDIFF(MILLISECOND, @start_time, SYSUTCDATETIME()) / 1000.0;

        SET @status_message = CONCAT('Success: ', @nashville_rows_inserted, ' nashville rows inserted.');   

        SELECT 
               @status AS [status],
               @status_message AS [returnMsg],
               @rows_total AS [rows_total],
               @exec_duration_seconds AS [execTime];
    END TRY

    BEGIN CATCH
        SET @error_message = ERROR_MESSAGE();
        SET @status_message = CONCAT('ERROR: ', @error_message);
        SET @status = -1;
        SET @rows_total = 0;
        SET @exec_duration_seconds = DATEDIFF(MILLISECOND, @start_time, SYSUTCDATETIME()) / 1000.0;

        SELECT 
            @status AS [status],
            @status_message AS [returnMsg],
            @rows_total AS [rowCount],
            @exec_duration_seconds AS [execTime];
    END CATCH
END;
GO
