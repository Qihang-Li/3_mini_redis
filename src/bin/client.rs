use bytes::Bytes;
use clap::{Parser, Subcommand};
use mini_redis::requester::Requester;
use std::error::Error;
use std::net::SocketAddr;
use std::println;
use tokio::time::Duration;

#[derive(Parser, Debug)]
struct Cli {
    /// The network address of the server
    #[clap(long, default_value = "127.0.0.1:6379")]
    addr: String,

    #[clap(subcommand)]
    command: CliCommand,
}

#[derive(Subcommand, Debug)]
enum CliCommand {
    Get { key: String },
    Set { key: String, value: String },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Step 1: fetch the user input as arguments
    let cli = Cli::parse();

    // Step 2: connect to the server
    let socket_addr: SocketAddr = cli.addr.parse().expect("Invalid socket address format");
    let mut requester = Requester::connect(socket_addr, Duration::from_secs(10)).await?;

    // Step 3: branch for `GET` or `SET`
    match cli.command {
        CliCommand::Get { key } => {
            // Step 4: deal with a get command
            match requester.get(&key).await? {
                Some(frame) => println!("{frame:?}"),
                None => println!("(nil)"), // Standard Redis output for missing key
            }
        }
        CliCommand::Set { key, value } => {
            // Step 5: deal with a set command
            requester.set(&key, Bytes::from(value)).await?;
            println!("OK");
        }
    }

    Ok(())
}
