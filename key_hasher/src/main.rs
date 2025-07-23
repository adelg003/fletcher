use bcrypt::{DEFAULT_COST, hash};
use clap::Parser;
use color_eyre::eyre;

#[derive(Parser)]
#[command(version)]
struct CliArg {
    /// Key to hash
    #[arg(short, long)]
    key: String,
}

/// Hashes a key using bcrypt with default cost
pub fn hash_key(key: &str) -> Result<String, eyre::Error> {
    let hash_str = hash(key, DEFAULT_COST)?;
    Ok(hash_str)
}

fn main() -> Result<(), eyre::Error> {
    // Lets get pretty error reports
    color_eyre::install()?;

    // Pull in arg and get the password we want to hash
    let arg = CliArg::parse();

    // Hash password using extracted function
    let hash_str = hash_key(&arg.key)?;

    // Print password_hash to screen
    println!("Key hash: {hash_str}");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bcrypt::verify;
    use pretty_assertions::assert_ne;

    #[test]
    fn test_hash_key_produces_valid_hash() {
        let test_key = "test_password_123";

        let result = hash_key(test_key);

        // Should not error
        assert!(
            result.is_ok(),
            "hash_key should successfully hash the input key without errors"
        );

        let hash = result.unwrap();

        // Hash should not be empty
        assert!(!hash.is_empty(), "Generated hash should not be empty");

        // Hash should not be the same as input
        assert_ne!(
            hash, test_key,
            "Generated hash should be different from the original key for security"
        );

        // Hash should be verifiable with the original key
        assert!(
            verify(test_key, &hash).unwrap(),
            "Generated hash should be verifiable with the original key"
        );
    }

    #[test]
    fn test_hash_key_different_inputs_produce_different_hashes() {
        let key1 = "password1";
        let key2 = "password2";

        let hash1 = hash_key(key1).unwrap();
        let hash2 = hash_key(key2).unwrap();

        // Different inputs should produce different hashes
        assert_ne!(
            hash1, hash2,
            "Different input keys should produce different hash outputs"
        );

        // Each hash should verify with its corresponding input
        assert!(
            verify(key1, &hash1).unwrap(),
            "First hash should verify correctly with its original key"
        );
        assert!(
            verify(key2, &hash2).unwrap(),
            "Second hash should verify correctly with its original key"
        );

        // Each hash should NOT verify with the other input
        assert!(
            !verify(key1, &hash2).unwrap(),
            "First key should NOT verify with the second key's hash (cross-verification should fail)"
        );
        assert!(
            !verify(key2, &hash1).unwrap(),
            "Second key should NOT verify with the first key's hash (cross-verification should fail)"
        );
    }

    #[test]
    fn test_hash_key_same_input_produces_different_salted_hashes() {
        let test_key = "same_password";

        let hash1 = hash_key(test_key).unwrap();
        let hash2 = hash_key(test_key).unwrap();

        // Due to salting, same input should produce different hashes
        assert_ne!(
            hash1, hash2,
            "Same input should produce different hashes due to bcrypt salting mechanism"
        );

        // But both should verify with the original key
        assert!(
            verify(test_key, &hash1).unwrap(),
            "First hash should verify with the original key despite different salts"
        );
        assert!(
            verify(test_key, &hash2).unwrap(),
            "Second hash should verify with the original key despite different salts"
        );
    }

    #[test]
    fn test_hash_key_empty_string() {
        let empty_key = "";

        let result = hash_key(empty_key);

        // Should still work with empty string
        assert!(
            result.is_ok(),
            "hash_key should handle empty string input without errors"
        );

        let hash = result.unwrap();
        assert!(
            !hash.is_empty(),
            "Hash of empty string should still produce a non-empty hash output"
        );
        assert!(
            verify(empty_key, &hash).unwrap(),
            "Hash of empty string should be verifiable with the empty string input"
        );
    }
}
