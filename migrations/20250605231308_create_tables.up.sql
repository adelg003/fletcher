-- Add up migration script here

-- Plan DAG Table
CREATE TABLE dag (
  dataset_id UUID PRIMARY KEY,
  plan_dag JSONB NOT NULL,
  paused BOOL NOT NULL,
  modified_by TEXT NOT NULL,
  modified_date TIMESTAMPTZ NOT NULL
);

-- GDS Compute Types
CREATE TYPE compute_type AS ENUM (
  'cams',
  'dbxaas'
);

-- Data Product States
CREATE TYPE state_type AS ENUM (
  'waiting', -- waiting on dependencies
  'queued', -- job submitted but not started
  'running', -- compute reports job starting
  'success', -- job succeed
  'failed', -- job failed
  'removed' -- node is not part of the plan dag
);

-- Node Details Table
CREATE TABLE node (
  dataset_id UUID NOT NULL,
  node_id TEXT NOT NULL,
  compute compute_type NOT NULL,
  data_product TEXT NOT NULL,
  version TEXT NOT NULL,
  eager BOOL NOT NULL,
  passthrough JSONB,
  state state_type NOT NULL,
  passback JSONB,
  modified_by TEXT NOT NULL,
  modified_date TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (dataset_id, node_id),
  FOREIGN KEY(dataset_id) REFERENCES dag(dataset_id)
);
