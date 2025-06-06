-- Add up migration script here

-- Plan DAG Table
CREATE TABLE dag (
  dag_id UUID PRIMARY KEY,
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
  'disabled' -- node is not part of the plan dag
);

-- Node Details Table
CREATE TABLE node (
  dag_id UUID NOT NULL,
  node_id TEXT NOT NULL,
  compute compute_type NOT NULL,
  data_product TEXT NOT NULL,
  version TEXT NOT NULL,
  eager BOOL NOT NULL,
  passthrough JSONB,
  state state_type NOT NULL,
  run_id TEXT,
  run_link TEXT,
  passback JSONB,
  modified_by TEXT NOT NULL,
  modified_date TIMESTAMPTZ NOT NULL,
  PRIMARY KEY(dag_id, node_id),
  FOREIGN KEY(dag_id) REFERENCES dag(dag_id)
);

-- Edge Details Table
CREATE TABLE edge (
  dag_id UUID NOT NULL,
  source_node_id UUID NOT NULL,
  dest_node_id UUID NOT NULL,
  PRIMARY KEY(dag_id, source_id, dest_id),
  FOREIGN KEY(dag_id) REFERENCES dag(dag_id),
  FOREIGN KEY(dag_id, source_id) REFERENCES node(dag_id, node_id),
  FOREIGN KEY(dag_id, dest_id) REFERENCES node(dag_id, node_id)
);
