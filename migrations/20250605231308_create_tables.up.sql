-- Add up migration script here

-- Plan DAG Table
CREATE TABLE dag (
  id UUID PRIMARY KEY,
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
  id UUID PRIMARY KEY,
  dag_id UUID NOT NULL,
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
  UNIQUE(id, dag_id),
  FOREIGN KEY(dag_id) REFERENCES dag(id)
);

-- Add index so we can get all the nodes for a dag quickly
CREATE INDEX node_dag_id ON node(dag_id);

-- Edge Details Table
CREATE TABLE edge (
  id UUID PRIMARY KEY,
  dag_id UUID NOT NULL,
  source_id UUID NOT NULL,
  dest_id UUID NOT NULL,
  UNIQUE(source_id, dest_id),
  FOREIGN KEY(dag_id) REFERENCES dag(id),
  FOREIGN KEY(source_id, dag_id) REFERENCES node(id, dag_id),
  FOREIGN KEY(dest_id, dag_id) REFERENCES node(id, dag_id)
);

-- Add index so we can get all the edges for a dag quickly
CREATE INDEX edge_dag_id ON edge(dag_id);
