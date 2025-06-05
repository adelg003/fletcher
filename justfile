# Install Just: https://github.com/casey/just

##########
## Rust ##
##########

# Build Debug Binary
build:
  cargo build

# Build Release Binary
build-release:
  cargo build --release

# Build and Run a Develop Binay
run:
  cargo run

# Build and Run a Release Binary
run-release:
  cargo run --release

# Check Rust Code
check:
  cargo check --locked

# Check Rust Code using the SQLx Cache
check_w_sqlx_cache:
  SQLX_OFFLINE=true cargo check --locked

# Check Rust Linting
clippy:
  cargo clippy --locked -- --deny warnings

# Check Rust Linting using SQLx Cache
clippy_w_sqlx_cache:
  SQLX_OFFLINE=true cargo clippy --locked -- --deny warnings

# Apply Rust Formating
fmt:
  cargo fmt --verbose

# Check Rust Formating
fmt-check:
  cargo fmt --check --verbose

# Check Rust Unittest
test:
  cargo test --locked

# Install SQLx-CLI
sqlx-install:
  cargo install sqlx-cli --locked

# SQLx DB Migration
sqlx-migrate:
  sqlx migrate run

# Refresh SQLx Cache
sqlx-prepare:
  cargo sqlx prepare

# Check SQLx Cache
sqlx-check:
  cargo sqlx prepare --check

# Install Cargo Deny
deny-install:
  cargo install cargo-deny --locked

# Check Rust advisories, bans, licenses, sources
deny:
  cargo deny check


################
## PostgreSQL ##
################

# Create the network that we allow others to connect to PostgreSQL
pg-init-network:
  docker network create pg_network

# Start a local PostgreSQL instance for development.
pg-start:
  docker run \
    -t \
    --detach \
    --rm \
    --name fletcher_postgresql \
    --network=pg_network \
    --env POSTGRES_USER=fletcher_user \
    --env POSTGRES_PASSWORD=password \
    --env POSTGRES_DB=fletcher_db \
    --volume fletcher_postgresql:/var/lib/postgresql/data \
    --publish 5432:5432 \
    docker.io/library/postgres:alpine

# Stop local PostgreSQL
pg-stop:
  docker stop fletcher_postgresql

# Create the network that we allow others to connect to PostgreSQL
pg-init-network-podman:
  podman network create pg_network

# Start local PostgreSQL via Podman
pg-start-podman:
  podman run \
    -dt \
    --rm \
    --name fletcher_postgresql \
    --network=pg_network \
    --env POSTGRES_USER=fletcher_user \
    --env POSTGRES_PASSWORD=password \
    --env POSTGRES_DB=fletcher_db \
    --volume fletcher_postgresql:/var/lib/postgresql/data \
    --publish 5432:5432 \
    docker.io/library/postgres:alpine

# Stop local PostgreSQL via Podman
pg-stop-podman:
  podman stop fletcher_postgresql

# Connect to PostgreSQL via Rainfrog (https://github.com/achristmascarl/rainfrog)
pg-cli:
  rainfrog \
    --username=fletcher_user \
    --password=password \
    --host=localhost \
    --port=5432 \
    --database=fletcher_db \
    --driver=postgres


############
## Docker ##
############

# Build the Docker image
docker-build:
  docker build \
  . \
  --file Containerfile \
  --secret id=ssh_key,src=$HOME/.ssh/id_rsa \
  --tag localhost/fletcher:latest

# Run the Docker container in Detached mode
docker-run:
  docker run \
    --name=fletcher \
    --detach \
    --rm \
    --network=pg_network \
    --publish=3000:3000 \
    --env DATABASE_URL=postgres://fletcher_user:password@fletcher_postgresql/fletcher_db \
    localhost/fletcher:latest

# Dump logs from container
docker-logs:
 docker logs fletcher

# Follow logs from container
docker-follow:
 docker logs --follow fletcher

# Kill the Detached Docker container
docker-kill:
  docker kill fletcher

# Test the Healthcheck and that the service came up (Docker only)
docker-healthcheck:
  sh ./scripts/test_healthcheck.sh

# Build the Docker image via Podman
podman-build:
  podman build \
  . \
  --file Containerfile \
  --secret id=ssh_key,src=$HOME/.ssh/id_rsa \
  --tag localhost/fletcher:latest

# Run the Docker container in Detached mode via Podman
podman-run:
  podman run \
    --name=fletcher \
    --detach \
    --rm \
    --network=pg_network \
    --publish=3000:3000 \
    --env DATABASE_URL=postgres://fletcher_user:password@fletcher_postgresql/fletcher_db \
    localhost/fletcher:latest

# Dump logs from container via Podman
podman-logs:
 podman logs fletcher

# Follow logs from container via Podman
podman-follow:
 podman logs --follow fletcher

# Kill the Detached Docker container via Podman
podman-kill:
  podman kill fletcher


###########
## Trivy ##
###########

# Trivy Scan the code repo
trivy-repo:
  trivy repo .

# Trivy Scan the Docker image
trivy-image:
  trivy image localhost/fletcher:latest


############
## Github ##
############

# Run all Github Rust Checks
github-rust-checks: sqlx-migrate sqlx-check check_w_sqlx_cache clippy_w_sqlx_cache fmt-check test deny

# Run all Github Docker Checks
github-docker-checks: docker-build docker-run docker-healthcheck docker-kill

# Run all Github Docker Checks via Podman
github-podman-checks: podman-build

# Run all Github Trivy Checks
github-trivy-checks: trivy-repo trivy-image

# Run all Github Checks
github-checks: github-rust-checks github-docker-checks github-trivy-checks

# Run all Github Checks via Podman
github-checks-podman: github-rust-checks github-podman-checks github-trivy-checks
