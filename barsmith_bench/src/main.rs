mod cli;
mod compare;
mod model;
mod runner;
mod suites;

use anyhow::Result;
use clap::Parser;

use cli::{Cli, Command};

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Run(args) => runner::run(args),
        Command::Compare(args) => compare::run(args),
    }
}
