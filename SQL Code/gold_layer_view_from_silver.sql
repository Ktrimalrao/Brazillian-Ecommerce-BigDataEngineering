create schema gold

create view gold.final 
as 
SELECT
     *
FROM
    OPENROWSET(
        BULK 'https://olistdatastorageacctiru.blob.core.windows.net/olistdata/Silver/',
        FORMAT = 'PARQUET'
    ) AS result1



select * from gold.final
