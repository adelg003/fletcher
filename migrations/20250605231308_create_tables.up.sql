-- Add up migration script here

-- Dataset Table
CREATE TABLE dataset (
  dataset_id UUID PRIMARY KEY,
  paused BOOL NOT NULL,
  extra JSONB,
  modified_by TEXT NOT NULL,
  modified_date TIMESTAMPTZ NOT NULL
);

-- GDS Compute Types
CREATE TYPE compute AS ENUM (
  'cams',
  'dbxaas'
);

-- Data Product States
CREATE TYPE state AS ENUM (
  'waiting', -- waiting on dependencies
  'queued', -- job submitted but not started
  'running', -- compute reports job starting
  'success', -- job succeed
  'failed', -- job failed
  'disabled' -- data_product is not part of the plan dataset
);

-- Data Product Table
CREATE TABLE data_product (
  dataset_id UUID NOT NULL,
  data_product_id TEXT NOT NULL,
  compute compute NOT NULL,
  name TEXT NOT NULL,
  version TEXT NOT NULL,
  eager BOOL NOT NULL,
  passthrough JSONB,
  state state NOT NULL,
  run_id UUID,
  link TEXT,
  passback JSONB,
  extra JSONB,
  modified_by TEXT NOT NULL,
  modified_date TIMESTAMPTZ NOT NULL,
  PRIMARY KEY(dataset_id, data_product_id),
  FOREIGN KEY(dataset_id) REFERENCES dataset(dataset_id)
);

-- Dependencies between Data Products Table
CREATE TABLE dependency (
  dataset_id UUID NOT NULL,
  parent_id TEXT NOT NULL,
  child_id TEXT NOT NULL,
  extra JSONB,
  modified_by TEXT NOT NULL,
  modified_date TIMESTAMPTZ NOT NULL,
  PRIMARY KEY(dataset_id, parent_id, child_id),
  FOREIGN KEY(dataset_id) REFERENCES dataset(dataset_id),
  FOREIGN KEY(dataset_id, parent_id) REFERENCES data_product(dataset_id, data_product_id),
  FOREIGN KEY(dataset_id, child_id) REFERENCES data_product(dataset_id, data_product_id)
);
