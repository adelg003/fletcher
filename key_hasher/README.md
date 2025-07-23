# Key Hasher

A simple command-line tool for generating bcrypt hashes from input keys/passwords.

## What it does

This tool takes a key or password as input and generates a secure bcrypt hash using the default cost factor (12). Each time you run it with the same input, you'll get a different hash due to the random salt that bcrypt automatically generates.

## TLDR

### Quick way (using just)
```bash
just hash "your_password_here"
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

## Features

- ✅ Secure bcrypt hashing with default cost factor (12)
- ✅ Handles empty strings, special characters, and Unicode
- ✅ Random salt ensures different hashes for same input
- ✅ Standard CLI argument parsing with help and version flags
- ✅ Comprehensive error handling

## Testing

The project includes comprehensive integration tests covering:

- Basic functionality (short and long flags)
- Hash quality verification
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
```
$2b$12$[22-character salt][31-character hash]
```

- Total length: 60 characters
- Cost factor: 12 (default)
- Random salt: ensures unique hashes for same input
- Compatible with standard bcrypt libraries

## Security Notes

- Uses bcrypt's default cost factor (12) which provides good security vs performance balance
- Each hash includes a random salt, so the same password will produce different hashes
- Generated hashes can be verified using any standard bcrypt library 