use assert_cmd::Command;
use bcrypt::verify;

#[test]
fn test_hash_key_with_short_flag() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();

    cmd.arg("-k").arg("test_password");

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(
        output.status.success(),
        "Command should succeed with valid key argument"
    );
    assert!(
        stdout.starts_with("Key hash: $2"),
        "Output should start with 'Key hash: $2', got: {}",
        stdout
    );
    assert!(
        stdout.contains("$2"),
        "Output should contain bcrypt identifier '$2', got: {}",
        stdout
    );
    assert!(
        stdout.ends_with('\n'),
        "Output should end with newline, got: {:?}",
        stdout
    );
}

#[test]
fn test_hash_key_with_long_flag() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();

    cmd.arg("--key").arg("another_test_password");

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(
        output.status.success(),
        "Command should succeed with valid --key argument"
    );
    assert!(
        stdout.starts_with("Key hash: $2"),
        "Output should start with 'Key hash: $2', got: {}",
        stdout
    );
    assert!(
        stdout.contains("$2"),
        "Output should contain bcrypt identifier '$2', got: {}",
        stdout
    );
    assert!(
        stdout.ends_with('\n'),
        "Output should end with newline, got: {:?}",
        stdout
    );
}

#[test]
fn test_different_keys_produce_different_hashes() {
    let mut cmd1 = Command::cargo_bin("key_hasher").unwrap();
    let mut cmd2 = Command::cargo_bin("key_hasher").unwrap();

    let output1 = cmd1.arg("-k").arg("password1").output().unwrap();
    let output2 = cmd2.arg("-k").arg("password2").output().unwrap();

    let hash1 = String::from_utf8(output1.stdout).unwrap();
    let hash2 = String::from_utf8(output2.stdout).unwrap();

    assert_ne!(
        hash1, hash2,
        "Different passwords should produce different hashes"
    );

    // Both should still be valid bcrypt hashes
    assert!(
        hash1.starts_with("Key hash: $2"),
        "First hash should be valid bcrypt format, got: {}",
        hash1
    );
    assert!(
        hash2.starts_with("Key hash: $2"),
        "Second hash should be valid bcrypt format, got: {}",
        hash2
    );
}

#[test]
fn test_same_key_produces_different_hashes_due_to_salt() {
    let mut cmd1 = Command::cargo_bin("key_hasher").unwrap();
    let mut cmd2 = Command::cargo_bin("key_hasher").unwrap();

    let output1 = cmd1.arg("-k").arg("same_password").output().unwrap();
    let output2 = cmd2.arg("-k").arg("same_password").output().unwrap();

    let hash1 = String::from_utf8(output1.stdout).unwrap();
    let hash2 = String::from_utf8(output2.stdout).unwrap();

    // Same password should produce different hashes due to random salt
    assert_ne!(
        hash1, hash2,
        "Same password should produce different hashes due to salt"
    );

    // Both should still be valid bcrypt hashes
    assert!(
        hash1.starts_with("Key hash: $2"),
        "First hash should be valid bcrypt format, got: {}",
        hash1
    );
    assert!(
        hash2.starts_with("Key hash: $2"),
        "Second hash should be valid bcrypt format, got: {}",
        hash2
    );
}

#[test]
fn test_missing_key_argument() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();

    let output = cmd.output().unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();

    assert!(
        !output.status.success(),
        "Command should fail when no key argument is provided"
    );
    assert!(
        stderr.contains("required arguments were not provided"),
        "Error message should mention required arguments, got: {}",
        stderr
    );
}

#[test]
fn test_empty_key() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();

    cmd.arg("-k").arg("");

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(
        output.status.success(),
        "Command should succeed with empty key"
    );
    assert!(
        stdout.starts_with("Key hash: $2"),
        "Empty key should still produce valid hash, got: {}",
        stdout
    );
}

#[test]
fn test_key_with_special_characters() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();

    cmd.arg("-k").arg("p@ssw0rd!#$%^&*()");

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(
        output.status.success(),
        "Command should succeed with special characters in key"
    );
    assert!(
        stdout.starts_with("Key hash: $2"),
        "Special characters should produce valid hash, got: {}",
        stdout
    );
}

#[test]
fn test_key_with_unicode_characters() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();

    cmd.arg("-k").arg("pãssw🔑rd");

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(
        output.status.success(),
        "Command should succeed with Unicode characters in key"
    );
    assert!(
        stdout.starts_with("Key hash: $2"),
        "Unicode characters should produce valid hash, got: {}",
        stdout
    );
}

#[test]
fn test_very_long_key() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();

    let long_key = "a".repeat(1000);
    cmd.arg("-k").arg(&long_key);

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(
        output.status.success(),
        "Command should succeed with very long key (1000 chars)"
    );
    assert!(
        stdout.starts_with("Key hash: $2"),
        "Long key should produce valid hash, got: {}",
        stdout
    );
}

#[test]
fn test_help_flag() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();

    cmd.arg("--help");

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(output.status.success(), "Help command should succeed");
    assert!(
        stdout.contains("Key to hash"),
        "Help text should contain argument description, got: {}",
        stdout
    );
    assert!(
        stdout.contains("Usage:"),
        "Help text should contain usage information, got: {}",
        stdout
    );
}

#[test]
fn test_help_short_flag() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();

    cmd.arg("-h");

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(output.status.success(), "Short help command should succeed");
    assert!(
        stdout.contains("Key to hash"),
        "Help text should contain argument description, got: {}",
        stdout
    );
    assert!(
        stdout.contains("Usage:"),
        "Help text should contain usage information, got: {}",
        stdout
    );
}

#[test]
fn test_version_flag() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();

    cmd.arg("--version");

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(output.status.success(), "Version command should succeed");
    assert!(
        stdout.contains("key_hasher"),
        "Version output should contain program name, got: {}",
        stdout
    );
    assert!(
        stdout.contains("0.1.0"),
        "Version output should contain version number, got: {}",
        stdout
    );
}

#[test]
fn test_version_short_flag() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();

    cmd.arg("-V");

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(
        output.status.success(),
        "Short version command should succeed"
    );
    assert!(
        stdout.contains("key_hasher"),
        "Version output should contain program name, got: {}",
        stdout
    );
    assert!(
        stdout.contains("0.1.0"),
        "Version output should contain version number, got: {}",
        stdout
    );
}

#[test]
fn test_invalid_argument() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();

    cmd.arg("--invalid-flag");

    let output = cmd.output().unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();

    assert!(
        !output.status.success(),
        "Command should fail with invalid argument"
    );
    assert!(
        stderr.contains("unexpected argument"),
        "Error message should mention unexpected argument, got: {}",
        stderr
    );
}

#[test]
fn test_bcrypt_hash_format() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();

    cmd.arg("-k").arg("format_test");

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(
        output.status.success(),
        "Command should succeed for hash format test"
    );

    // Extract just the hash part (remove "Key hash: " prefix and newline)
    let hash_line = stdout.strip_prefix("Key hash: ").unwrap();
    let hash = hash_line.trim();

    // Bcrypt hashes should be 60 characters long and start with $2 (or variants)
    assert_eq!(
        hash.len(),
        60,
        "Bcrypt hash should be 60 characters long, got: {} chars in '{}'",
        hash.len(),
        hash
    );
    assert!(
        hash.starts_with("$2"),
        "Bcrypt hash should start with '$2', got: '{}'",
        hash
    );

    // Should contain exactly 3 dollar signs (format: $2x$cost$salthash)
    let dollar_count = hash.chars().filter(|&c| c == '$').count();
    assert_eq!(
        dollar_count, 3,
        "Bcrypt hash should contain exactly 3 dollar signs, got: {} in '{}'",
        dollar_count, hash
    );
}

#[test]
fn test_hash_verification() {
    let test_password = "verification_test";
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();

    cmd.arg("-k").arg(test_password);

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(
        output.status.success(),
        "Command should succeed for hash verification test"
    );

    // Extract the hash
    let hash_line = stdout.strip_prefix("Key hash: ").unwrap();
    let hash = hash_line.trim();

    // Verify that the hash actually matches the original password
    assert!(
        verify(test_password, hash).unwrap(),
        "Generated hash '{}' should verify against original password '{}'",
        hash,
        test_password
    );

    // Verify that it doesn't match a different password
    assert!(
        !verify("wrong_password", hash).unwrap(),
        "Generated hash '{}' should NOT verify against wrong password 'wrong_password'",
        hash
    );
}
