use bytes::Bytes;
use clap::{Parser, Subcommand};
use mini_redis::config;
use mini_redis::requester::Requester;
use std::error::Error;
use std::net::SocketAddr;

#[derive(Parser, Debug)]
struct Cli {
    /// Server IP address and port, e.g. `127.0.0.1:6379` or `[::1]:6379`.
    /// Hostnames are not supported.
    #[clap(long, default_value = config::DEFAULT_CLIENT_ADDR)]
    addr: SocketAddr,

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

    // Step 2: Connect to the server
    let server_addr: SocketAddr = cli.addr;
    let mut requester = Requester::connect(server_addr, config::DEFAULT_CLIENT_IO_TIMEOUT).await?;

    // Step 3: Execute the selected command
    match cli.command {
        // GET command
        CliCommand::Get { key } => match requester.get(&key).await? {
            // Print the returned bytes using debug formatting.
            Some(value) => println!("{value:?}"),
            // Display a missing key as `(nil)`.
            None => println!("(nil)"),
        },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_rejects_invalid_addr() {
        let result = Cli::try_parse_from(["client", "--addr", "not-an-address", "get", "Answer"]);
        assert!(result.is_err());
    }
}
