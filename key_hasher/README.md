# Key Hasher

A simple command-line tool for generating bcrypt hashes from input keys/passwords.

## What it does

This tool takes a key or password as input and generates a secure bcrypt hash
using a configurable cost factor (default: 12). Each time you run it with the
same input, you'll get a different hash due to the random salt that bcrypt
automatically generates.

## TLDR

### Quick way (using just)

```bash
# Default cost (12)
just hash "your_password_here"

# Custom cost
just hash "your_password_here" 10
```

## Build

```bash
cargo build --release --package key_hasher
```

## Usage

The binary will be available at `target/release/key_hasher`.

### Direct binary usage

#### Hash a key using short flag

```bash
key_hasher -k "your_password_here"
```

#### Hash a key using long flag

```bash
key_hasher --key "your_password_here"
```

#### Hash a key with custom cost factor

```bash
# Using short flags
key_hasher -k "your_password_here" -c 10

# Using long flags
key_hasher --key "your_password_here" --cost 8
```

#### Get help

```bash
key_hasher --help
key_hasher -h
```

#### Get version

```bash
key_hasher --version
key_hasher -V
```

### Development usage

```bash
# Run without building release
cargo run --package key_hasher -- --key "your_password_here"

# Run with custom cost factor
cargo run --package key_hasher -- --key "your_password_here" --cost 10

# Get help in development
cargo run --package key_hasher -- --help

# Get version in development  
cargo run --package key_hasher -- --version
```

## Example Output

### Using just command

```bash
$ just hash "test_password"
Key hash: $2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/lewi5FjZX7ksw/xG6
```

### Using just command with custom cost

```bash
$ just hash "test_password" 10
Key hash: $2b$10$QOZi5S2w0LlHq0fVXAu7/uHDBV2oRWrwbhjTeDIIyOsFrwcWgnkeu
```

### Using direct binary

```bash
$ key_hasher -k "test_password"
Key hash: $2b$12$uM2B9UcBlN9G8RqgPTJoLeJ9rYD6PJ2XITVtBlQAbWoAWoC44ELb.
```

### Using cargo run

```bash
$ cargo run --package key_hasher -- --key "test_password"
Key hash: $2b$12$0zve.dkiJOtq/bOxhRyth.t0eKYWAzZBcoIs/MTwmDhfD1CJY0bj6
```

### Using custom cost factor

```bash
$ key_hasher -k "test_password" -c 8
Key hash: $2b$08$5zve.dkiJOtq/bOxhRyth.t0eKYWAzZBcoIs/MTwmDhfD1CJY0bj6
```

## Features

- ✅ Secure bcrypt hashing with configurable cost factor (default: 12)
- ✅ Customizable security level via `--cost` flag (range: 4-31)
- ✅ Handles empty strings, special characters, and Unicode
- ✅ Random salt ensures different hashes for same input
- ✅ Standard CLI argument parsing with help and version flags
- ✅ Comprehensive error handling

## Testing

The project includes comprehensive integration tests covering:

- Basic functionality (short and long flags)
- Cost factor functionality (`--cost` flag with various values)
- Hash quality verification and cost validation
- Edge cases (empty keys, special characters, Unicode, long inputs)
- CLI behavior (help, version, error handling)
- Hash format validation
- Actual hash verification using bcrypt

### Run all tests

```bash
cargo test
```

### Run specific test

```bash
cargo test test_hash_key_with_short_flag
```

### Run tests with output

```bash
cargo test -- --nocapture
```

## Dependencies

- `bcrypt` - For secure password hashing
- `clap` - For command-line argument parsing
- `color-eyre` - For pretty error reporting

### Development Dependencies

- `assert_cmd` - For CLI testing

## Hash Format

The tool generates standard bcrypt hashes in the format:

```text
$2b$[cost]$[22-character salt][31-character hash]
```

- Total length: 60 characters
- Cost factor: configurable (default: 12, range: 4-31)
- Random salt: ensures unique hashes for same input
- Compatible with standard bcrypt libraries

**Examples by cost factor:**

- Cost 4: `$2b$04$...` (fast, testing only)
- Cost 12: `$2b$12$...` (default, good balance)
- Cost 15: `$2b$15$...` (higher security)

## Security Notes

- Uses bcrypt's default cost factor (12) which provides good security vs
  performance balance
- Cost factor can be customized from 4 (fast, testing) to 31 (very secure)
- Higher cost factors exponentially increase computation time and security
- Each hash includes a random salt, so the same password will produce
  different hashes
- Generated hashes can be verified using any standard bcrypt library

**Cost Factor Guidelines:**

- **4-6**: Testing and development only
- **10-12**: Production use (good balance)
- **13-15**: High security applications
- **16+**: Very high security (longer computation time)
