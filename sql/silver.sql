CREATE TABLE iceberg.silver.customer_nation
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT c.custkey, c.name, c.acctbal, n.name AS nation,
       c.address, c.phone, c.comment
FROM iceberg.bronze.customer c
JOIN iceberg.bronze.nation n ON c.nationkey=n.nationkey;
CREATE TABLE iceberg.silver.returned_order_items
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT o.custkey, o.orderkey, o.orderdate, l.linenumber,
       l.extendedprice, l.discount,
       l.extendedprice * (1-l.discount) AS net_revenue
FROM iceberg.bronze.orders o
JOIN iceberg.bronze.lineitem l ON l.orderkey=o.orderkey
WHERE o.orderdate >= DATE '1993-10-01'
  AND o.orderdate < DATE '1994-01-01'
  AND l.returnflag='R'
  AND l.extendedprice IS NOT NULL AND l.extendedprice >= 0
  AND l.discount IS NOT NULL AND l.discount BETWEEN 0 AND 1;
