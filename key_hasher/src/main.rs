use bcrypt::{DEFAULT_COST, hash};
use clap::Parser;
use color_eyre::eyre;

#[derive(Parser)]
#[command(version)]
struct CliArg {
    /// Key to hash
    #[arg(short, long)]
    key: String,
    #[arg(short, long)]
    cost: Option<u32>,
}

fn main() -> Result<(), eyre::Error> {
    // Lets get pretty error reports
    color_eyre::install()?;

    // Pull in arg and get the password we want to hash and hard we want to hash it
    let arg = CliArg::parse();
    let cost: u32 = arg.cost.unwrap_or(DEFAULT_COST);

    // Hash password
    let hash_str: String = hash(arg.key, cost)?;

    // Print password_hash to screen
    println!("Key hash: {hash_str}");

    Ok(())
}
