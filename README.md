# Fletcher

Fletcher is a data orchestration platform that uses in-memory directed acyclic graphs (DAGs) to orchestrate the triggering of compute jobs. With its precise orchestration, your data products won't rush or drag — no one can say "Not quite my tempo."

## What is Fletcher?

Fletcher manages **Plans** - collections of **Data Products** organized into **Datasets** with **Dependencies** that form a DAG. When data products succeed, Fletcher automatically triggers downstream jobs that are ready to run, ensuring efficient and reliable data pipeline execution.

### Key Concepts

- **Dataset**: A container for a plan that can be paused/unpaused
- **Data Product**: An individual compute job with states (waiting, queued, running, success, failed, disabled)
- **Dependencies**: Parent-child relationships between data products that form the execution DAG
- **Plan**: The complete specification of data products and their dependencies for a dataset

## Features

- 🎯 **DAG-based Orchestration**: Automatically resolves dependencies and triggers ready jobs
- 🔄 **Real-time State Management**: Track and update data product states with automatic downstream triggering
- 🌐 **REST API**: Full OpenAPI/Swagger documented REST interface
- 🖥️ **Web UI**: Search, visualize, and manage your data pipelines
- 🐘 **PostgreSQL Backend**: Reliable data persistence with migrations
- 🔍 **Cycle Detection**: Validates DAGs to prevent infinite loops
- ⏸️ **Pause/Resume**: Control dataset execution flow
- 🧪 **Multiple Compute Types**: Support for CAMS and DBXaaS compute platforms
- 📊 **GraphViz Visualization**: Visual representation of your DAG execution plans

## Quick Start

### Prerequisites

- Rust (2024 edition)
- PostgreSQL
- Docker/Podman (optional)
- Just command runner (optional but recommended)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd fletcher
   ```

2. **Set up PostgreSQL**
   ```bash
   # Using Just (recommended)
   just pg-start
   
   # Or manually with Docker
   docker run -d --name fletcher_postgresql \
     --env POSTGRES_USER=fletcher_user \
     --env POSTGRES_PASSWORD=password \
     --env POSTGRES_DB=fletcher_db \
     --publish 5432:5432 \
     postgres:alpine
   ```

3. **Configure environment**

   **Option A: Using .env file (Recommended for development)**
   ```bash
   # Create a .env file in the project root
   cat > .env << 'EOF'
   DATABASE_URL=postgres://fletcher_user:password@localhost/fletcher_db
   RUST_BACKTRACE=1
   EOF
   ```

   **Option B: Manual export**
   ```bash
   export DATABASE_URL="postgres://fletcher_user:password@localhost/fletcher_db"
   ```

4. **Run database migrations**
   ```bash
   just sqlx-migrate
   # Or: sqlx migrate run
   ```

5. **Build and run**
   ```bash
   just run
   # Or: cargo run
   ```

The application will be available at `http://localhost:3000`

## Web User Interface

Fletcher provides a modern, responsive web interface for managing and visualizing your data orchestration pipelines.

### Features

- **🔍 Live Search**: Real-time search for plans with instant results
- **📊 DAG Visualization**: Interactive GraphViz diagrams showing data product dependencies
- **📋 Plan Management**: Detailed view of datasets, data products, and their states
- **🎨 Modern Design**: Beautiful gradient styling with smooth animations
- **📱 Responsive Layout**: Works seamlessly across desktop and mobile devices

### Pages

#### Search Page (`/`)
The main landing page provides plan discovery functionality:

- **Live Search**: Type-ahead search with 500ms debounce for finding plans
- **Real-time Results**: HTMX-powered dynamic updates without page refreshes
- **Paginated Results**: Efficient loading of large result sets (50 items per page)
- **Quick Navigation**: Click any result to instantly jump to the plan details

#### Plan Page (`/plan/{dataset_id}`)
Comprehensive plan visualization and management:

- **Dataset Overview**: 
  - Dataset ID and current status (Active/Paused)
  - Last modified information
  - Quick status indicators with colored badges

- **Interactive DAG Visualization**:
  - GraphViz-powered dependency graph
  - Color-coded nodes by state:
    - 🟢 Green: Success
    - 🟡 Light Green: Running  
    - ⚪ Light Grey: Waiting
    - ⚫ Grey: Queued/Disabled
    - 🔴 Red: Failed
  - Left-to-right flow layout for clear dependency understanding

- **Data Products Table**:
  - Complete data product inventory
  - State badges with color coding
  - Compute platform indicators (CAMS/DBXaaS)
  - Eager execution flags
  - Direct links to external systems
  - Last modification timestamps

- **Technical Details**:
  - Pretty-printed JSON payload with syntax highlighting
  - Complete plan specification for debugging and analysis

### Technology Stack

- **🎨 TailwindCSS**: Modern utility-first CSS framework for responsive design
- **⚡ HTMX**: Progressive enhancement for dynamic interactions without complex JavaScript
- **📈 GraphViz**: Professional dependency graph visualization with Viz.js
- **🌈 Prism.js**: Beautiful syntax highlighting for JSON payloads
- **🖼️ Maud**: Type-safe HTML templating in Rust

### Navigation

- **Breadcrumb Navigation**: Clear path between Search and Plan pages
- **Contextual Links**: Smart navigation that adapts based on current context
- **Direct URLs**: Bookmarkable URLs for all plans and searches

### Accessibility

- **Semantic HTML**: Proper heading hierarchy and ARIA labels
- **Keyboard Navigation**: Full keyboard accessibility for all interactive elements
- **Screen Reader Support**: Descriptive text and proper labeling
- **Color Contrast**: High contrast design for visibility

### Browser Support

Fletcher's UI works in all modern browsers with:
- ES6+ JavaScript support
- SVG rendering capabilities
- CSS Grid and Flexbox support

### UI Endpoints

- `/` - Main search interface
- `/plan/{dataset_id}` - Plan details and visualization
- `/component/plan_search` - HTMX search component
- `/assets/*` - Static assets (CSS, JS, images)

## API Endpoints

### Plans
- `POST /api/plan` - Create or update a plan
- `GET /api/plan/{dataset_id}` - Get a plan by dataset ID
- `GET /api/plan/search` - Search plans
- `PUT /api/plan/pause/{dataset_id}` - Pause/unpause a dataset

### Data Products
- `GET /api/data_product/{dataset_id}/{data_product_id}` - Get a data product
- `PUT /api/data_product/update/{dataset_id}` - Update data product states
- `PUT /api/data_product/clear/{dataset_id}` - Clear data products and downstream dependencies
- `PUT /api/data_product/disable/{dataset_id}` - Disable data products

### Documentation
- `/swagger` - Interactive API documentation
- `/spec` - OpenAPI specification

## Usage Examples

### Creating a Plan

```json
{
  "dataset": {
    "id": "123e4567-e89b-12d3-a456-426614174000",
    "extra": {}
  },
  "data_products": [
    {
      "id": "223e4567-e89b-12d3-a456-426614174000",
      "compute": "cams",
      "name": "Extract Raw Data",
      "version": "1.0.0",
      "eager": true,
      "passthrough": {},
      "extra": {}
    },
    {
      "id": "323e4567-e89b-12d3-a456-426614174000",
      "compute": "dbxaas",
      "name": "Transform Data",
      "version": "1.0.0",
      "eager": true,
      "passthrough": {},
      "extra": {}
    }
  ],
  "dependencies": [
    {
      "parent_id": "223e4567-e89b-12d3-a456-426614174000",
      "child_id": "323e4567-e89b-12d3-a456-426614174000",
      "extra": {}
    }
  ]
}
```

### Data Product States

Fletcher manages the following states:
- `waiting` - Waiting on dependencies to complete
- `queued` - Job submitted but not started
- `running` - Compute reports job is running
- `success` - Job completed successfully
- `failed` - Job failed
- `disabled` - Data product is not part of the active plan

## Development

### Prerequisites

Install development dependencies:
```bash
# Install Just command runner
cargo install just

# Install SQLx CLI
just sqlx-install

# Install cargo-deny for security checks
just deny-install
```

### Environment Configuration

Fletcher uses environment variables for configuration. For local development, create a `.env` file in the project root:

```bash
# .env file for local development
DATABASE_URL=postgres://fletcher_user:password@localhost/fletcher_db
RUST_BACKTRACE=1

# Optional: Set log levels
RUST_LOG=debug
```

**Available Environment Variables:**
- `DATABASE_URL` - PostgreSQL connection string (required)
- `RUST_BACKTRACE` - Set to `1` or `full` for detailed error traces
- `RUST_LOG` - Log level (`error`, `warn`, `info`, `debug`, `trace`)

**Note:** The `.env` file is automatically loaded by Fletcher using the `dotenvy` crate.

### Development Commands

```bash
# Build
just build                    # Debug build
just build-release           # Release build

# Run
just run                     # Run with debug
just run-release             # Run optimized

# Testing
just test                    # Run all tests
just check                   # Check code compilation
just clippy                  # Run linter
just fmt                     # Format code
just fmt-check               # Check formatting

# Database
just sqlx-migrate            # Run migrations
just sqlx-revert             # Revert last migration
just sqlx-reset              # Reset database
just sqlx-prepare            # Update SQLx cache
just sqlx-check              # Verify SQLx cache

# Security
just deny                    # Check dependencies for security issues
just trivy-repo              # Scan repository
just trivy-image             # Scan Docker image

# PostgreSQL Development
just pg-start                # Start PostgreSQL container
just pg-stop                 # Stop PostgreSQL container
just pg-cli                  # Connect with rainfrog CLI

# Docker/Podman
just docker-build            # Build Docker image
just docker-run              # Run Docker container
just podman-build            # Build with Podman
just podman-run              # Run with Podman

# Utilities
just hash "password"         # Generate bcrypt hash
```

### Project Structure

```
fletcher/
├── src/
│   ├── api.rs              # REST API endpoints
│   ├── core.rs             # Business logic
│   ├── dag.rs              # DAG operations and validation
│   ├── db.rs               # Database operations
│   ├── error.rs            # Error handling
│   ├── model.rs            # Data models
│   ├── main.rs             # Application entry point
│   └── ui/                 # Web UI components
├── migrations/             # Database migrations
├── key_hasher/             # Password hashing utility
├── scripts/                # Utility scripts
├── assets/                 # Static web assets
└── justfile                # Development commands
```

### Testing

Run the comprehensive test suite:
```bash
# All tests
just test

# With coverage
cargo test --workspace

# Integration tests
cargo test --test integration_tests
```

### Database Schema

Fletcher uses PostgreSQL with the following main tables:

- `dataset` - Dataset metadata and pause state
- `data_product` - Individual data products with state tracking
- `dependency` - Parent-child relationships between data products

See `migrations/` for complete schema definitions.

## Deployment

### Docker

```bash
# Build image
just docker-build

# Run with PostgreSQL
just pg-start
just docker-run

# Health check
just docker-healthcheck
```

### Environment Variables

**Required:**
- `DATABASE_URL` - PostgreSQL connection string

**Optional:**
- `RUST_BACKTRACE` - Set to `1` or `full` for detailed error traces  
- `RUST_LOG` - Log level (`error`, `warn`, `info`, `debug`, `trace`)

**For Development:** Use a `.env` file (see Environment Configuration section above)  
**For Production:** Set environment variables directly in your deployment system

## Configuration

Fletcher supports compute types:
- `cams` - C-AMS compute platform
- `dbxaas` - DBXaaS compute platform

Plans can include custom JSON metadata in `extra` fields for extensibility.

## Monitoring

- Health check endpoint: `GET /` (returns 200 when healthy)
- Logs: Structured logging with tracing
- Metrics: HTTP request tracing via middleware

## Contributing

1. Fork the repository
2. Create a feature branch
3. Run tests: `just test`
4. Run linting: `just clippy`
5. Check formatting: `just fmt-check`
6. Run security checks: `just deny`
7. Submit a pull request

### Code Quality

The project maintains high code quality standards:
- Comprehensive test coverage
- Strict linting with Clippy
- Security scanning with cargo-deny and Trivy
- Automated CI/CD with GitHub Actions

## Why is this repo called Fletcher?

This repo is named after Terence Fletcher, who was the world ~~infamous~~ famous conductor of the Shaffer Studio Jazz Band at the Shaffer Conservatory in New York City. Just as Fletcher demanded perfect timing and precision from his musicians, this orchestration platform ensures your data products execute with perfect timing and precision.

## License

See the license field in `Cargo.toml` for licensing information.
