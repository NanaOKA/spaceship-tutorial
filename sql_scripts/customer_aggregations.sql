
--------------- GET CUSTOMERS AND TRANSACTIONS OF INTEREST ---------------
DROP TABLE IF EXISTS #customer_transactions;
CREATE TABLE #customer_transactions AS
(
    SELECT
        cust.mstr_customer_id,
        cust.original_purchase_date,
        cust.business_unit,
        trx.order_id,
        trx.transaction_date,
        trx.amount,
        trx.sku,
        trx.quantity,
        trx.quantity*trx.amount as spend
    FROM $[database_name].$[customer_info_table] cust
    INNER JOIN $[database_name].$[transaction_table] trx
    ON
        cust.mstr_customer_id = trx.mstr_customer_id AND
        cust.business_unit = trx.business_unit
    WHERE
        -- Filter for only NA business units
        (cust.business_unit = $[bu_a] OR cust.business_unit = $[bu_b] OR cust.business_unit = $[bu_c]) AND
        trx.transaction_date >= '$[purchase_start_date]' AND
        trx.transaction_date < '$[purchase_end_date]' AND
        -- Filter for orders and purchases
        trx.transaction_sub_type = 1 AND
        trx.sales_credit = 1
);


---------------CUSTOMER SPEND AGGREGATES --------------------------------------------------------
DROP TABLE IF EXISTS #customer_spend_aggregates;
CREATE TABLE #customer_spend_aggregates AS
(
    SELECT
        mstr_customer_id,
        business_unit,
        MIN(spend) as minimum_spend,
        MAX(spend) as maximum_spend,
        AVG(spend) as average_spend,
        STDDEV(spend) as stddev_total_spend,
        SUM(spend) as total_spend,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY spend) as median_spend,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY spend) as spend_percentile_25,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY spend) as spend_percentile_75
    FROM
        #customer_transactions
    GROUP BY mstr_customer_id, business_unit
);

-------------------------------------------------------------------------------------------------
-- SELECT * from #customer_spend_aggregates LIMIT 100;

UNLOAD ('SELECT * from #customer_spend_aggregates;')
TO 's3://$[pdc_slides_bucket]$[region]/$[customer_group]/customer_spend_aggregates.csv'
iam_role 'arn:aws:iam::773919108708:role/Redshift-Spectrum-Role' CSV header parallel off allowoverwrite;
