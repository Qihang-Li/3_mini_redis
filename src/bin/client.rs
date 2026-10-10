use bytes::Bytes;
use clap::{Parser, Subcommand};
use mini_redis::config;
use mini_redis::requester::Requester;
use std::error::Error;
use std::net::SocketAddr;

#[derive(Parser, Debug)]
struct Cli {
    /// The network address of the server
    #[clap(long, default_value = config::DEFAULT_CLIENT_ADDR)]
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
    // Step 1: Parse the command-line arguments
    let cli = Cli::parse();

    // Step 2: Parse the socket address and connect to the server
    let socket_addr: SocketAddr = cli.addr.parse().expect("Invalid socket address format");
    let mut requester = Requester::connect(socket_addr, config::DEFAULT_CLIENT_IO_TIMEOUT).await?;

    // Step 3: Execute the selected command
    match cli.command {
        // GET command
        CliCommand::Get { key } => {
            match requester.get(&key).await? {
                // Print the returned bytes using debug formatting.
                Some(frame) => println!("{frame:?}"),
                // Display a missing key as `(nil)`.
                None => println!("(nil)"),
            }
        }
        // SET command
        CliCommand::Set { key, value } => {
            // Send the value as UTF-8 bytes.
            requester.set(&key, Bytes::from(value)).await?;
            // Print success after the expected `OK` response arrives.
            println!("OK");
        }
    }

    Ok(())
}
