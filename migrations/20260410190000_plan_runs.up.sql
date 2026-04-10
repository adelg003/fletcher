CREATE TABLE plan_run (
  plan_run_id UUID PRIMARY KEY,
  dataset_id UUID NOT NULL,
  status state NOT NULL,
  created_by TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  modified_by TEXT NOT NULL,
  modified_date TIMESTAMPTZ NOT NULL,
  FOREIGN KEY(dataset_id) REFERENCES dataset(dataset_id)
);

CREATE INDEX plan_run_dataset_created_idx
  ON plan_run(dataset_id, created_at DESC);

CREATE TABLE plan_run_data_product (
  plan_run_id UUID NOT NULL,
  dataset_id UUID NOT NULL,
  data_product_id UUID NOT NULL,
  compute compute NOT NULL,
  name TEXT NOT NULL,
  version TEXT NOT NULL,
  eager BOOL NOT NULL,
  state state NOT NULL,
  step_run_id UUID,
  link TEXT,
  passback JSONB,
  modified_by TEXT NOT NULL,
  modified_date TIMESTAMPTZ NOT NULL,
  PRIMARY KEY(plan_run_id, data_product_id),
  FOREIGN KEY(plan_run_id) REFERENCES plan_run(plan_run_id) ON DELETE CASCADE
);

CREATE INDEX plan_run_data_product_step_run_idx
  ON plan_run_data_product(step_run_id);

CREATE TABLE plan_run_dependency (
  plan_run_id UUID NOT NULL,
  dataset_id UUID NOT NULL,
  parent_id UUID NOT NULL,
  child_id UUID NOT NULL,
  PRIMARY KEY(plan_run_id, parent_id, child_id),
  FOREIGN KEY(plan_run_id) REFERENCES plan_run(plan_run_id) ON DELETE CASCADE
);
