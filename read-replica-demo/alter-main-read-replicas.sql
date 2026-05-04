-- Enable read replicas (followers) on an existing row table: 3 read replicas per availability zone.
-- Adjust the table path to match your database (default matches key_prefix_demo from sample-ddl.sql).
--
-- Docs: https://ydb.tech/docs/en/concepts/datamodel/table?version=v25.3#read_only_replicas
-- Note: READ_REPLICAS_SETTINGS cannot be reset once set.

ALTER TABLE `key_prefix_demo/main` SET (
  READ_REPLICAS_SETTINGS = "PER_AZ: 3"
);
