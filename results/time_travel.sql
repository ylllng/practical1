SELECT * FROM iceberg.demo.customer_nation FOR VERSION AS OF 2884159182053332328;
ALTER TABLE iceberg.demo.customer_nation EXECUTE rollback_to_snapshot(2884159182053332328);
