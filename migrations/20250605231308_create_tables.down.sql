-- Add down migration script here

-- Drop Node table and types
DROP TABLE node;
DROP TYPE state_type;
DROP TYPE compute_type;

-- Drop Plan Dag Table
DROP TABLE dag;
