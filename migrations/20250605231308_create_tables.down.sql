-- Add down migration script here

-- Drop dependency table
DROP TABLE dependency;

-- Drop Data Product table and types
DROP TABLE data_product;
DROP TYPE state;
DROP TYPE compute;

-- Drop Dataset Table
DROP TABLE dataset;
