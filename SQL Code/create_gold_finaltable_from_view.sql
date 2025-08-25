-- STEP 1: Create master key (if not already created)
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'd8TySvmWr4rPBUk';


-- STEP 2: Create credential using managed identity
CREATE DATABASE SCOPED CREDENTIAL trimaladmin WITH IDENTITY = 'Managed Identity';


-- STEP 3: Drop external file format if it already exists
IF EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'extfileformat')
    DROP EXTERNAL FILE FORMAT extfileformat;

-- STEP 4: Drop external data source if it already exists
IF EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'goldlayer')
    DROP EXTERNAL DATA SOURCE goldlayer;



-- STEP 5: Create external file format for Parquet with Snappy compression
CREATE EXTERNAL FILE FORMAT extfileformat 
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);


-- STEP 6: Create external data source pointing to the Gold folder
CREATE EXTERNAL DATA SOURCE goldlayer 
WITH (
    LOCATION = 'https://olistdatastorageacctiru.dfs.core.windows.net/olistdata/Gold/',
    CREDENTIAL = trimaladmin
);


-- STEP 7: Create external table using the Serving folder
CREATE EXTERNAL TABLE gold.finaltable
WITH (
    LOCATION = 'Serving',
    DATA_SOURCE = goldlayer,
    FILE_FORMAT = extfileformat
)
AS
SELECT * FROM gold.final;

select * from gold.finaltable

select * from gold.final

