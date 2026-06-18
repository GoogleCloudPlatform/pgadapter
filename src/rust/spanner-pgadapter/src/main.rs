// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(author, version, about = "Rust PGAdapter Proxy for Cloud Spanner", long_about = None)]
struct Args {
    /// GCP project ID
    #[arg(short, long)]
    project: Option<String>,

    /// Spanner instance ID
    #[arg(short, long)]
    instance: Option<String>,

    /// Spanner database ID
    #[arg(short, long)]
    database: Option<String>,

    /// Path to service account credentials file
    #[arg(short, long)]
    credentials_file: Option<String>,

    /// Server port to listen on
    #[arg(short = 's', long, default_value_t = 5432)]
    server_port: u16,

    /// Spanner gRPC endpoint
    #[arg(short = 'e', long)]
    spanner_endpoint: Option<String>,

    /// Perform authentication step
    #[arg(short, long, default_value_t = false)]
    authenticate: bool,

    /// SSL Mode configuration
    #[arg(long)]
    ssl: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Initialize tracing subscriber from environment (defaulting to info)
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let server = spanner_pgadapter::server::ProxyServer::bind(
        args.project,
        args.instance,
        args.database,
        args.spanner_endpoint,
        args.server_port,
    )
    .await?;

    server.start().await?;
    Ok(())
}
