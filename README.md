# Fletcher

Fletcher is a data orchestration platform that uses in-memory directed acyclic
graphs (DAGs) to orchestrate the triggering of compute jobs. With its precise
orchestration, your data products won't rush or drag — no one can say "Not
quite my tempo."

## What is Fletcher?

Fletcher manages **Plans** - collections of **Data Products** organized into
**Datasets** with **Dependencies** that form a DAG. When data products succeed,
Fletcher automatically triggers downstream jobs that are ready to run, ensuring
efficient and reliable data pipeline execution.

### Key Concepts

- **Dataset**: A container for a plan that can be paused/unpaused
- **Data Product**: An individual compute job with states (waiting, queued,
  running, success, failed, disabled)
- **Dependencies**: Parent-child relationships between data products that form
  the execution DAG
- **Plan**: The complete specification of data products and their dependencies
  for a dataset

## Features

- 🎯 **DAG-based Orchestration**: Automatically resolves dependencies and
  triggers ready jobs
- 🔄 **Real-time State Management**: Track and update data product states with
  automatic downstream triggering
- 🌐 **REST API**: Full OpenAPI/Swagger documented REST interface
- 🖥️ **Web UI**: Search, visualize, and manage your data pipelines
- 🐘 **PostgreSQL Backend**: Reliable data persistence with migrations
- 🔍 **Cycle Detection**: Validates DAGs to prevent infinite loops
- ⏸️ **Pause/Resume**: Control dataset execution flow
- 🧪 **Multiple Compute Types**: Support for CAMS and DBXaaS compute platforms
- 📊 **GraphViz Visualization**: Visual representation of your DAG execution plans
- 🚀 **Load Testing**: Built-in Locust-based performance testing with realistic scenarios

## Quick Start

### Prerequisites

- Rust (2024 edition)
- PostgreSQL
- Docker/Podman (optional)
- Just command runner (optional but recommended)

### Development Prerequisites

Additional tools for development and testing:

- Node.js and npm (for frontend CSS build)
- Python 3.13+ and uv (for load testing)
- SQLx CLI (for database operations)

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
   SECRET_KEY=your-secret-key-for-jwt-signing-make-it-long-and-random
   REMOTE_APIS='[
     {
       "service": "local",
       "hash": "$2b$10$DvqWB.sMjo1XSlgGrOzGAuBTY5E1hkLiDK3BdcK0TiROjCWkgCeaa",
       "roles": ["publish", "pause", "update", "disable"]
     },
     {
       "service": "readonly",
       "hash": "$2b$10$46TiUvUaKvp2D/BuoXe8Fu9ktffCBXioF8M0DeeOWvz8X2J0RtpvK",
       "roles": []
     }
   ]'
   RUST_BACKTRACE=1
   EOF
   ```

   **Note**: The above configuration includes:
   - `local` service with password `abc123` (full access)
   - `readonly` service with password `abc123` (read-only access)
   - Generate new password hashes with: `just hash "your-password"`

   **Option B: Manual export**

   ```bash
   export DATABASE_URL="postgres://fletcher_user:password@localhost/fletcher_db"
   export SECRET_KEY="your-secret-key-for-jwt-signing-make-it-long-and-random"
   export REMOTE_APIS='[{"service":"local","hash":"$2b$10$DvqWB.sMjo1XSlgGrOzGAuBTY5E1hkLiDK3BdcK0TiROjCWkgCeaa","roles":["publish","pause","update","disable"]},{"service":"readonly","hash":"$2b$10$46TiUvUaKvp2D/BuoXe8Fu9ktffCBXioF8M0DeeOWvz8X2J0RtpvK","roles":[]}]'
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

Fletcher provides a modern, responsive web interface for managing and
visualizing your data orchestration pipelines.

### UI Features

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
- **⚡ HTMX**: Progressive enhancement for dynamic interactions without
  complex JavaScript
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

### Frontend Build Process

Fletcher's UI uses modern frontend tools for styling and interactivity:

#### CSS Framework

- **TailwindCSS 4.x** - Utility-first CSS framework with modern features
- **DaisyUI** - Component library built on TailwindCSS
- **TailwindCSS Animated** - Animation utilities

#### JavaScript Libraries

- **HTMX** - Progressive enhancement for dynamic interactions
- **Viz.js** - GraphViz rendering for dependency graphs
- **Prism.js** - Syntax highlighting for JSON payloads

#### Development Workflow

The CSS is built using TailwindCSS CLI and bundled with the Rust application:

```bash
# Install Node.js dependencies
npm install

# Build CSS (automatically handled during Rust build)
# See build.rs for integration details
```

The `build.rs` script automatically handles CSS compilation during the Rust
build process, ensuring the latest styles are always included in the binary.

## Authentication

Fletcher uses **JWT (JSON Web Token) authentication** with **role-based
access control (RBAC)** to secure API endpoints.

### Authentication Flow

1. **Get JWT Token**

   ```bash
   curl -X POST http://localhost:3000/api/authenticate \
     -H "Content-Type: application/json" \
     -d '{
       "service": "local",
       "key": "abc123"
     }'
   ```

2. **Response**

   ```json
   {
     "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
     "token_type": "Bearer",
     "expires": 1640995200,
     "issued": 1640991600,
     "issued_by": "Fletcher",
     "ttl": 3600,
     "service": "local",
     "roles": ["disable", "pause", "publish", "update"]
   }
   ```

3. **Use Bearer Token**

   ```bash
   curl -X POST http://localhost:3000/api/plan \
     -H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9..." \
     -H "Content-Type: application/json" \
     -d '{ ... }'
   ```

### Roles and Permissions

Fletcher implements four distinct roles that control access to different operations:

<!-- markdownlint-disable MD013 -->
| Role          | Description                 | Endpoints                                   |
|---------------|-----------------------------|---------------------------------------------|
| **`publish`** | Create and submit new plans | `POST /api/plan`                            |
| **`update`**  | Modify data product states  | `PUT /api/data_product/{dataset_id}/update` |
|               |                             | `PUT /api/data_product/{dataset_id}/clear`  |
| **`pause`**   | Pause and unpause datasets  | `PUT /api/plan/{dataset_id}/pause`          |
|               |                             | `PUT /api/plan/{dataset_id}/unpause`        |
| **`disable`** | Disable data products       | `DELETE /api/data_product/{dataset_id}`     |
<!-- markdownlint-enable MD013 -->

### Service Configuration

Services are configured via the `REMOTE_APIS` environment variable:

```json
[
  {
    "service": "local",
    "hash": "$2b$10$DvqWB.sMjo1XSlgGrOzGAuBTY5E1hkLiDK3BdcK0TiROjCWkgCeaa",
    "roles": ["publish", "pause", "update", "disable"]
  },
  {
    "service": "readonly", 
    "hash": "$2b$10$46TiUvUaKvp2D/BuoXe8Fu9ktffCBXioF8M0DeeOWvz8X2J0RtpvK",
    "roles": []
  }
]
```

- **`local`** - Full access service with all roles
- **`readonly`** - Limited access service with no modification roles
- **`hash`** - bcrypt hash of the service's password (use
  `just hash "password"` to generate)

## API Endpoints

### Authentication Endpoints

- `POST /api/authenticate` - Get JWT token (no auth required)

### Plans

- `POST /api/plan` - Create or update a plan **[Requires: `publish` role]**
- `GET /api/plan/{dataset_id}` - Get a plan by dataset ID
- `GET /api/plan/search` - Search plans  
- `PUT /api/plan/{dataset_id}/pause` - Pause a dataset **[Requires: `pause` role]**
- `PUT /api/plan/{dataset_id}/unpause` - Unpause a dataset **[Requires: `pause` role]**

### Data Products

- `GET /api/data_product/{dataset_id}/{data_product_id}` - Get a data product
- `PUT /api/data_product/{dataset_id}/update` - Update data product states
  **[Requires: `update` role]**
- `PUT /api/data_product/{dataset_id}/clear` - Clear data products and
  downstream dependencies **[Requires: `update` role]**
- `DELETE /api/data_product/{dataset_id}` - Disable data products
  **[Requires: `disable` role]**

### Documentation

- `/swagger` - Interactive API documentation
- `/spec` - OpenAPI specification

## Usage Examples

### Authentication Workflow

1. **Authenticate and get JWT**

   ```bash
   # Get JWT token
   curl -X POST http://localhost:3000/api/authenticate \
     -H "Content-Type: application/json" \
     -d '{
       "service": "local", 
       "key": "abc123"
     }'
   ```

2. **Extract token from response**

   ```json
   {
     "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
     "token_type": "Bearer",
     "service": "local",
     "roles": ["disable", "pause", "publish", "update"]
   }
   ```

3. **Use token for authenticated requests**

   ```bash
   # Store token in variable
   TOKEN="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9..."
   
   # Create a plan (requires 'publish' role)
   curl -X POST http://localhost:3000/api/plan \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d @plan.json
   ```

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

### Installation and Setup

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

Fletcher uses environment variables for configuration. For local development,
create a `.env` file in the project root:

```bash
# .env file for local development
DATABASE_URL=postgres://fletcher_user:password@localhost/fletcher_db
SECRET_KEY=your-secret-key-for-jwt-signing-make-it-long-and-random
REMOTE_APIS='[
  {
    "service": "local",
    "hash": "$2b$10$DvqWB.sMjo1XSlgGrOzGAuBTY5E1hkLiDK3BdcK0TiROjCWkgCeaa",
    "roles": ["publish", "pause", "update", "disable"]
  },
  {
    "service": "readonly",
    "hash": "$2b$10$46TiUvUaKvp2D/BuoXe8Fu9ktffCBXioF8M0DeeOWvz8X2J0RtpvK",
    "roles": []
  }
]'
RUST_BACKTRACE=1

# Optional: Set log levels
RUST_LOG=debug
```

**Available Environment Variables:**

- `DATABASE_URL` - PostgreSQL connection string (required)
- `MAX_CONNECTIONS` - Number of PostgreSQL connections in the pool
  (default is 10)
- `SECRET_KEY` - Secret key for JWT token signing (required)
- `REMOTE_APIS` - JSON array of service configurations with roles
  (required)
- `RUST_BACKTRACE` - Set to `1` or `full` for detailed error traces
- `RUST_LOG` - Log level (`error`, `warn`, `info`, `debug`, `trace`)

**Note:** The `.env` file is automatically loaded by Fletcher using the
`dotenvy` crate.

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

# Load Testing & Stress Testing
just run-stress              # Run Fletcher with stress test settings (30 connections)
just locust                  # Run basic load test
just locust-demo             # Run continuous demo (1 user, API only)
just locust-stress           # Run stress test (2000 users, continuous loop)
just locust-busiest-day      # Run busiest day test (300 users, single run)

# Python Development
just py-right-check          # Type checking with pyright
just py-right-check-watch    # Watch mode type checking
just py-ruff-check           # Linting with ruff
just py-ruff-fix             # Auto-fix with ruff
just py-ruff-fmt             # Format with ruff
just py-ruff-fmt-check       # Check formatting with ruff
just py-ruff-check-watch     # Watch mode linting
just py-audit                # Scan for vulnerable dependencies

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

```text
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
├── locust/                 # Load testing framework
│   ├── src/
│   │   ├── locustfile.py   # Main load testing scenarios
│   │   ├── model.py        # Pydantic models for API data
│   │   └── setup.py        # Test data generation utilities
│   ├── pyproject.toml      # Python project configuration
│   └── README.md           # Load testing documentation
├── migrations/             # Database migrations
├── key_hasher/             # Password hashing utility
├── scripts/                # Utility scripts
├── assets/                 # Static web assets
├── package.json            # Node.js dependencies (CSS build)
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
- `SECRET_KEY` - Secret key for JWT token signing (generate a long, random string)
- `REMOTE_APIS` - JSON array of service configurations with authentication and roles

**Optional:**

- `MAX_CONNECTIONS` - Number of PostgreSQL connections in the pool. Each connection
  uses one core in PostgreSQL (default is 10)
- `RUST_BACKTRACE` - Set to `1` or `full` for detailed error traces  
- `RUST_LOG` - Log level (`error`, `warn`, `info`, `debug`, `trace`)

**For Development:** Use a `.env` file (see Environment Configuration
section above)  
**For Production:** Set environment variables directly in your deployment
system

**Security Notes:**

- Use a strong, randomly generated `SECRET_KEY` in production
- Store bcrypt password hashes in `REMOTE_APIS`, never plain text passwords
- Use `just hash "your-password"` to generate secure password hashes

## Load Testing

Fletcher includes comprehensive load testing capabilities using **Locust** to simulate
realistic user workflows and evaluate system performance under various loads.

### Load Testing Features

- **🎯 Realistic Workflows**: Simulates authentic Fletcher API usage patterns
- **🔄 Plan Lifecycle Testing**: Creates plans, updates data products, and
  manages states
- **🌐 UI Testing**: Tests both API endpoints and web interface interactions
- **📊 Multiple Test Modes**: Support for single-run and continuous loop testing
- **⚡ Performance Metrics**: Detailed insights into system behavior under load
- **🔧 Configurable Parameters**: Customizable authentication, hosts, and
  execution modes

### Load Testing Quick Start

1. **Run basic load test**

   ```bash
   just locust
   ```

2. **Run continuous demo (1 user, API only)**

   ```bash
   just locust-demo
   ```

3. **Run Fletcher in stress test mode**

   ```bash
   just run-stress
   ```

4. **Run stress test (2500 users, continuous loop)**

   ```bash
   just locust-stress
   ```

5. **Run busiest day simulation (300 users, single run)**

   ```bash
   just locust-busiest-day
   ```

### Load Test Configuration

Pre-configured test scenarios:

```bash
just locust                  # Interactive load test
just locust-demo             # Continuous demo (1 user)
just locust-stress           # Stress test (2500 users)
just locust-busiest-day      # Production simulation (300 users)
just run-stress              # Run Fletcher with optimized settings
```

**Available Parameters:**

- `--host` - Base URL of the Fletcher API server
- `--users` - Number of concurrent users to simulate
- `--spawn-rate` - Users spawned per second
- `--mode` - Execution mode: `once` or `loop`
- `--service` - Service name for authentication (default: `local`)
- `--key` - Authentication key for the service (default: `abc123`)
- `--autostart` - Start test automatically without web UI interaction

### Test Scenarios

The load tests simulate several realistic Fletcher workflows:

1. **Authentication Flow**: Get JWT tokens and verify role-based access
2. **Plan Creation**: Submit complex plans with multiple data products and dependencies
3. **State Management**: Update data product states and trigger downstream jobs
4. **UI Interaction**: Navigate the web interface, search plans, and view visualizations
5. **Cleanup Operations**: Clear data products and reset states for continuous testing

### Performance Benchmarks

**Key Results**: Fletcher achieves optimal performance with `MAX_CONNECTIONS=30`,
supporting 1,300 concurrent users (180 requests/second). PostgreSQL CPU is the
primary limiting factor.

#### Key Performance Metrics

- **Optimal Performance**: 1,300 concurrent users (180 requests/second)
- **Configuration**: `MAX_CONNECTIONS=30` (optimized from default 10)
- **Busiest Day Simulation**: 300 users representing peak daily load
  (completes in 8m3s)
- **Resource Usage**:
  - Fletcher: 28-160MB RAM (depending on load)
  - PostgreSQL: 1.2-5GB RAM during stress testing

#### Stress Test Findings

- **Database-Bound Performance**: PostgreSQL CPU is the primary limiting factor
- **Connection Pool Impact**: Increasing connections from 10 to 30 provides:
  - +37% improvement in concurrent user capacity (950 → 1,300 users)
  - +29% improvement in requests/second (140 → 180 RPS)
  - -42% reduction in Fletcher memory usage (275MB → 160MB)
- **Scalability Ceiling**: Performance plateaus around 2,500+ users due to
  database constraints

#### Production Recommendations

- **Use `MAX_CONNECTIONS=30`** for optimal performance
- **Plan for ~1,000-1,300 concurrent users** with current architecture
- **Monitor PostgreSQL CPU and connection utilization** as key metrics

**Test Environment:**

- **CPU**: AMD Ryzen 9 7950X (32 cores) @ 5.88 GHz
- **Storage**: 2TB NVMe Gen 4 SSD (Sabrent)
- **Database**: PostgreSQL running locally (same machine)
- **RAM**: 64GB total available

**Detailed Results**: For comprehensive stress test analysis, performance
comparisons, and architectural considerations, see
[`locust/results/notes.md`](locust/results/notes.md).

**Performance Charts**:

![30 Connections Test (Recommended)](locust/results/stress_30_connections/stress_test_30_connections_rps_chart.png)
*30 Connections Test - Optimal configuration achieving 180 RPS*

![10 Connections Test (Default)](locust/results/stress_10_connections/stress_test_10_connections_rps_chart.png)
*10 Connections Test - Default configuration achieving 140 RPS*

![Busiest Day Simulation](locust/results/busiest_day/busiest_day_test_rps_chart.png)
*Busiest Day Simulation - 300 users representing peak daily load*

**Load Test Reports**: Interactive HTML reports with detailed statistics:

- [30 Connections Test Report](locust/results/stress_30_connections/stress_test_30_connections.html)
  (recommended configuration)
- [10 Connections Test Report](locust/results/stress_10_connections/stress_test_10_connections.html)
  (default configuration)
- [Busiest Day Test Report](locust/results/busiest_day/busiest_day_test.html)
  (production load simulation)

Additional files (CSV data, charts) are organized in the
[`locust/results/`](locust/results/) folder by test type.

*Note: Performance may vary significantly based on hardware specifications,
network latency (if using remote PostgreSQL), and system load.*

### Stress Testing Setup

For accurate stress testing, run Fletcher with optimized settings:

```bash
# Terminal 1: Start Fletcher with stress test configuration
just run-stress

# Terminal 2: Run stress test
just locust-stress
```

The `run-stress` command configures Fletcher with `MAX_CONNECTIONS=30` for
optimal database performance under high load.

### Load Testing Development

Load testing is built with modern Python tools:

```bash
# Install dependencies
uv --directory locust/ sync

# Type checking
just py-right-check

# Linting and formatting
just py-ruff-check
just py-ruff-fix
just py-ruff-fmt

# Security scanning
just py-audit

# Interactive development
just py-ruff-check-watch
just py-right-check-watch
```

**Test Results**: Locust automatically generates comprehensive HTML reports.
See the Performance Benchmarks section above for direct links to interactive
reports.

### Load Test Architecture

The load testing framework includes:

- **`locustfile.py`** - Main test scenarios and user behavior simulation
- **`model.py`** - Pydantic models for Fletcher API data structures  
- **`setup.py`** - Test data generation utilities for complex plans
- **`pyproject.toml`** - Python project configuration with dependencies

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
6. Run Python checks: `just py-right-check py-ruff-check py-ruff-fmt-check py-audit`
7. Test load performance: `just locust-busiest-day`
8. Run security checks: `just deny`
9. Submit a pull request

### Code Quality

The project maintains high code quality standards:

- Comprehensive test coverage (Rust and Python)
- Strict linting with Clippy (Rust) and Ruff (Python)
- Type checking with pyright for Python code
- Security scanning with cargo-deny and Trivy
- Automated CI/CD with GitHub Actions

## Why is this repo called Fletcher?

This repo is named after Terence Fletcher, who was the world ~~infamous~~
famous conductor of the Shaffer Studio Jazz Band at the Shaffer Conservatory
in New York City. Just as Fletcher demanded perfect timing and precision from
his musicians, this orchestration platform ensures your data products execute
with perfect timing and precision.

## License

This project is licensed under the AGPL (Affero General Public License). View
the [LICENSE](LICENSE) file for more details.
