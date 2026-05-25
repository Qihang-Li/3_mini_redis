use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

fn main() {
    // Construct a subscriber that prints formatted traces to standard output
    let subscriber = FmtSubscriber::builder()
        // Define the maximum log level to record (TRACE, DEBUG, INFO, WARN, ERROR)
        .with_max_level(Level::INFO)
        // Complete the builder and return the subscriber
        .finish();

    // Set the subscriber as the global default for this application
    tracing::subscriber::set_global_default(subscriber)
        // Fail information
        .expect("Failed to set tracing subscriber");

    // Emit a test log to verify initialization
    info!("Mini-Redis observability layer successfully initialized.");
}
