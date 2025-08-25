SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://olistdatastorageacctiru.blob.core.windows.net/olistdata/Bronze/olist_customers_dataset.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0'
    ) AS [result]
