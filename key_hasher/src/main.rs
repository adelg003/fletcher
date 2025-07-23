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

fn main() -> Result<(), eyre::Error> {
    // Lets get pretty error reports
    color_eyre::install()?;

    // Pull in arg and get the password we want to hash
    let arg = CliArg::parse();

    // Hash password
    let hash_str: String = hash(arg.key, DEFAULT_COST)?;

    // Print password_hash to screen
    println!("Key hash: {hash_str}");

    Ok(())
}
