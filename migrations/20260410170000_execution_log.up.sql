CREATE TABLE execution_log (
  log_id BIGSERIAL PRIMARY KEY,
  dataset_id UUID NOT NULL,
  data_product_id UUID NOT NULL,
  run_id UUID NOT NULL,
  stream TEXT NOT NULL,
  message TEXT NOT NULL,
  created_by TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  FOREIGN KEY(dataset_id, data_product_id) REFERENCES data_product(dataset_id, data_product_id)
);

CREATE INDEX execution_log_dataset_product_run_idx
  ON execution_log (dataset_id, data_product_id, run_id, log_id);
