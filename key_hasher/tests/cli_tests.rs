use assert_cmd::Command;
use bcrypt::verify;
use predicates::str::{contains, ends_with, starts_with};
use pretty_assertions::{assert_eq, assert_ne};

#[test]
fn test_hash_key_with_short_flag() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();
    cmd.arg("-k").arg("test_password");

    cmd.assert()
        .success()
        .stdout(starts_with("Key hash: $2"))
        .stdout(contains("$2"))
        .stdout(ends_with("\n"));
}

#[test]
fn test_hash_key_with_long_flag() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();
    cmd.arg("--key").arg("another_test_password");

    cmd.assert()
        .success()
        .stdout(starts_with("Key hash: $2"))
        .stdout(contains("$2"))
        .stdout(ends_with("\n"));
}

#[test]
fn test_different_keys_produce_different_hashes() {
    let mut cmd1 = Command::cargo_bin("key_hasher").unwrap();
    cmd1.arg("-k").arg("password1");

    let mut cmd2 = Command::cargo_bin("key_hasher").unwrap();
    cmd2.arg("-k").arg("password2");

    let run1 = cmd1.assert().success().stdout(starts_with("Key hash: $2"));
    let run2 = cmd2.assert().success().stdout(starts_with("Key hash: $2"));

    let hash1 = String::from_utf8(run1.get_output().stdout.clone()).unwrap();
    let hash2 = String::from_utf8(run2.get_output().stdout.clone()).unwrap();

    assert_ne!(
        hash1, hash2,
        "Different passwords should produce different hashes"
    );
}

#[test]
fn test_same_key_produces_different_hashes_due_to_salt() {
    let mut cmd1 = Command::cargo_bin("key_hasher").unwrap();
    cmd1.arg("-k").arg("same_password");

    let mut cmd2 = Command::cargo_bin("key_hasher").unwrap();
    cmd2.arg("-k").arg("same_password");

    let run1 = cmd1.assert().success().stdout(starts_with("Key hash: $2"));
    let run2 = cmd2.assert().success().stdout(starts_with("Key hash: $2"));

    let hash1 = String::from_utf8(run1.get_output().stdout.clone()).unwrap();
    let hash2 = String::from_utf8(run2.get_output().stdout.clone()).unwrap();

    assert_ne!(
        hash1, hash2,
        "Same password should produce different hashes due to salt"
    );
}

#[test]
fn test_missing_key_argument() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();

    cmd.assert()
        .failure()
        .stderr(contains("required arguments were not provided"));
}

#[test]
fn test_empty_key() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();
    cmd.arg("-k").arg("");

    cmd.assert()
        .success()
        .stdout(starts_with("Key hash: $2"));
}

#[test]
fn test_key_with_special_characters() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();
    cmd.arg("-k").arg("p@ssw0rd!#$%^&*()");

    cmd.assert()
        .success()
        .stdout(starts_with("Key hash: $2"));
}

#[test]
fn test_key_with_unicode_characters() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();
    cmd.arg("-k").arg("pãssw🔑rd");

    cmd.assert()
        .success()
        .stdout(starts_with("Key hash: $2"));
}

#[test]
fn test_very_long_key() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();
    let long_key = "a".repeat(1000);
    cmd.arg("-k").arg(&long_key);

    cmd.assert()
        .success()
        .stdout(starts_with("Key hash: $2"));
}

#[test]
fn test_help_flag() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();
    cmd.arg("--help");

    cmd.assert()
        .success()
        .stdout(contains("Key to hash"))
        .stdout(contains("Usage:"));
}

#[test]
fn test_help_short_flag() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();
    cmd.arg("-h");

    cmd.assert()
        .success()
        .stdout(contains("Key to hash"))
        .stdout(contains("Usage:"));
}

#[test]
fn test_version_flag() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();
    cmd.arg("--version");

    cmd.assert()
        .success()
        .stdout(contains("key_hasher"))
        .stdout(contains("0.1.0"));
}

#[test]
fn test_version_short_flag() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();
    cmd.arg("-V");

    cmd.assert()
        .success()
        .stdout(contains("key_hasher"))
        .stdout(contains("0.1.0"));
}

#[test]
fn test_invalid_argument() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();
    cmd.arg("--invalid-flag");

    cmd.assert()
        .failure()
        .stderr(contains("unexpected argument"));
}

#[test]
fn test_bcrypt_hash_format() {
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();
    cmd.arg("-k").arg("format_test");

    let run = cmd.assert().success().stdout(starts_with("Key hash: $2"));

    let stdout = String::from_utf8(run.get_output().stdout.clone()).unwrap();

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
        "Bcrypt hash should start with '$2', got: '{hash}'"
    );

    // Should contain exactly 3 dollar signs (format: $2x$cost$salthash)
    let dollar_count = hash.chars().filter(|&c| c == '$').count();
    assert_eq!(
        dollar_count, 3,
        "Bcrypt hash should contain exactly 3 dollar signs, got: {dollar_count} in '{hash}'"
    );
}

#[test]
fn test_hash_verification() {
    let test_password = "verification_test";
    let mut cmd = Command::cargo_bin("key_hasher").unwrap();
    cmd.arg("-k").arg(test_password);

    let run = cmd.assert().success().stdout(starts_with("Key hash: $2"));

    let stdout = String::from_utf8(run.get_output().stdout.clone()).unwrap();

    // Extract the hash
    let hash_line = stdout.strip_prefix("Key hash: ").unwrap();
    let hash = hash_line.trim();

    // Verify that the hash actually matches the original password
    assert!(
        verify(test_password, hash).unwrap(),
        "Generated hash '{hash}' should verify against original password '{test_password}'"
    );

    // Verify that it doesn't match a different password
    assert!(
        !verify("wrong_password", hash).unwrap(),
        "Generated hash '{hash}' should NOT verify against wrong password 'wrong_password'"
    );
}
