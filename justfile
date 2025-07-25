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
  RUST_BACKTRACE=full cargo run

# Build and Run a Release Binary
run-release:
  RUST_BACKTRACE=full cargo run --release

# Check Rust Code
check:
  cargo check --workspace --locked

# Check Rust Code using the SQLx Cache
check_w_sqlx_cache:
  SQLX_OFFLINE=true cargo check --locked

# Check Rust Linting
clippy:
  cargo clippy --workspace --locked --all-targets -- --deny warnings

# Check Rust Linting using SQLx Cache
clippy_w_sqlx_cache:
  SQLX_OFFLINE=true cargo clippy --workspace --locked -- --deny warnings

# Apply Rust Formating
fmt:
  cargo fmt --verbose

# Check Rust Formating
fmt-check:
  cargo fmt --check --verbose

# Check Rust Unittest
test:
  cargo test --workspace --locked

# Install SQLx-CLI
sqlx-install:
  cargo install sqlx-cli --locked

# SQLx DB Migration
sqlx-migrate:
  sqlx migrate run

# SQLx DB Revert
sqlx-revert:
  sqlx migrate revert

# SQLx DB Reset
sqlx-reset:
  sqlx database reset

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
## Key Hasher ##
################

# Hash a given key (or any value realy)
hash key:
  RUST_BACKTRACE=full cargo run --package key_hasher -- --key {{ key }}


##############
## Markdown ##
##############

# Lint all Markdown files
markdownlint:
  markdownlint-cli2 "**/*.md" "#node_modules"

# Fix lints for Markdown files
markdownlint-fix:
  markdownlint-cli2 --fix "**/*.md" "#node_modules"


################
## PostgreSQL ##
################

# Create the network that we allow others to connect to PostgreSQL
pg-init-network client="docker":
  {{ client }} network create pg_network

# Create the network that we allow others to connect to PostgreSQL
pg-init-network-podman: (pg-init-network "podman")

# Start a local PostgreSQL instance for development.
pg-start client="docker":
  {{ client }} run \
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

# Start local PostgreSQL via Podman
pg-start-podman: (pg-start "podman")

# Stop local PostgreSQL
pg-stop client="docker":
  {{ client }} stop fletcher_postgresql

# Stop local PostgreSQL via Podman
pg-stop-podman: (pg-stop "podman") 

# Connect to PostgreSQL via Rainfrog (https://github.com/achristmascarl/rainfrog)
pg-cli:
  rainfrog \
    --username=fletcher_user \
    --password=password \
    --host=localhost \
    --port=5432 \
    --database=fletcher_db \
    --driver=postgres


#####################
## Docker / Podman ##
#####################

# Build the Docker image in release mode
docker-build client="docker" mode="release":
  {{ client }} build \
  . \
  --file Containerfile \
  --tag localhost/fletcher:{{ mode }} \
  --build-arg BUILD_MODE={{ mode }}

# Build the Docker image in debug mode
docker-build-debug: (docker-build "docker" "debug")

# Build the Docker image via Podman in release mode
podman-build: (docker-build "podman" "release")

# Build the Docker image via Podman in debug mode
podman-build-debug: (docker-build "podman" "debug")

# Run the Docker container in Detached mode
docker-run client="docker" mode="release":
  {{ client }} run \
    --name=fletcher \
    --detach \
    --rm \
    --network=pg_network \
    --publish=3000:3000 \
    --env DATABASE_URL='postgres://fletcher_user:password@fletcher_postgresql/fletcher_db' \
    --env SECRET_KEY='GRNr3wdyenBu9BJW3TNQLene6b2xij1avk4UmPnrBkFbOKM5883EZUncLgeSdwLs63Wg21tbBV2WqanwTAXtqloXHkmLLiecDsxH' \
    --env REMOTE_APIS='[{"service": "remote", "hash": "$2b$10$4i5iCctUtWc5szV6M8CJNur1ng2md/gT372tlOv6BemwLryOw5ZGu", "roles": ["publish", "pause", "update", "disable"]}]' \
    localhost/fletcher:{{ mode }}

# Run the Docker debug container in Detached mode
docker-run-debug: (docker-run "docker" "debug")

# Run the Docker container in Detached mode via Podman in release mode
podman-run: (docker-run "podman" "release")

# Run the Docker container in Detached mode via Podman in debug mode
podman-run-debug: (docker-run "podman" "debug")

# Dump logs from container
docker-logs client="docker":
 {{ client }} logs fletcher

# Dump logs from container via Podman
podman-logs: (docker-logs "podman")

# Follow logs from container
docker-follow client="docker":
 {{ client }} logs --follow fletcher

# Follow logs from container via Podman
podman-follow: (docker-follow "podman")

# Kill the Detached Docker container
docker-kill client="docker":
  {{ client }} kill fletcher

# Kill the Detached Docker container via Podman
podman-kill: (docker-kill "podman")

# Test the Healthcheck and that the service came up (Docker only)
docker-healthcheck:
  sh ./scripts/healthcheck.sh


###########
## Trivy ##
###########

# Trivy Scan the code repo
trivy-repo:
  trivy repo .

# Trivy Scan the Docker image
trivy-image mode="release":
  trivy image localhost/fletcher:{{ mode }}

# Trivy Scan the debug Docker image
trivy-image-debug: (trivy-image "debug")


############
## Github ##
############

# Run all Github Rust Checks
github-rust-checks: sqlx-check check_w_sqlx_cache clippy_w_sqlx_cache fmt-check test deny

# Run all Github Markdown Checks
github-markdown-checks: markdownlint

# Run all Github Docker Checks
github-docker-checks mode="debug": (docker-build "docker" mode) (docker-run "docker" mode) docker-healthcheck (docker-kill "docker")

# Run all Github Docker Checks via Podman (excluding Healthcheck)
github-podman-checks: (docker-build "podman" "debug")

# Run all Github Trivy Checks
github-trivy-checks client="docker": trivy-repo (docker-build client "debug") (trivy-image "debug")

# Run all Github Trivy Checks (via Podman)
github-trivy-checks-podman: (github-trivy-checks "podman")

# Run all Github Checks
github-checks: github-rust-checks github-markdown-checks github-docker-checks (github-trivy-checks "docker")

# Run all Github Checks (with Podman)
github-checks-podman: github-rust-checks github-markdown-checks github-podman-checks (github-trivy-checks "podman")
