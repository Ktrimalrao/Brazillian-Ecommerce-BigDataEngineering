SELECT
     *
FROM
    OPENROWSET(
        BULK 'https://olistdatastorageacctiru.blob.core.windows.net/olistdata/Silver/',
        FORMAT = 'PARQUET'
    ) AS result1