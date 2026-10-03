CREATE TABLE iceberg.gold.q10_top20
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT c.custkey, c.name, sum(f.net_revenue) AS revenue,
       c.acctbal, c.nation, c.address, c.phone, c.comment
FROM iceberg.silver.customer_nation c
JOIN iceberg.silver.returned_order_items f ON f.custkey=c.custkey
GROUP BY c.custkey, c.name, c.acctbal, c.nation, c.address, c.phone, c.comment
ORDER BY revenue DESC, c.custkey
LIMIT 20;
