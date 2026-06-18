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

use crate::backend::spanner::SpannerBackend;
use crate::pgwire::Connection;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::{error, info};

/// Life cycle manager of the stateful PGAdapter proxy server.
pub struct ProxyServer {
    project: Option<String>,
    instance: Option<String>,
    database: Option<String>,
    spanner_endpoint: Option<String>,
    listener: TcpListener,
}

impl ProxyServer {
    /// Binds the PGAdapter proxy server to the specified TCP port.
    pub async fn bind(
        project: Option<String>,
        instance: Option<String>,
        database: Option<String>,
        spanner_endpoint: Option<String>,
        port: u16,
    ) -> Result<Self, anyhow::Error> {
        let addr = format!("127.0.0.1:{}", port);
        let listener = TcpListener::bind(&addr).await?;
        Ok(Self {
            project,
            instance,
            database,
            spanner_endpoint,
            listener,
        })
    }

    /// Returns the local socket address this server is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr, anyhow::Error> {
        self.listener.local_addr().map_err(Into::into)
    }

    /// Starts the proxy server and enters the main accept-loop to process incoming client connections.
    pub async fn start(self) -> Result<(), anyhow::Error> {
        let local_addr = self.local_addr()?;
        info!("PGAdapter proxy server started on {}", local_addr);

        loop {
            match self.listener.accept().await {
                Ok((stream, _)) => {
                    let project = self.project.clone();
                    let instance = self.instance.clone();
                    let database = self.database.clone();
                    let spanner_endpoint = self.spanner_endpoint.clone();

                    tokio::spawn(async move {
                        let mut connection = Connection::new(
                            stream,
                            SpannerBackend::new(),
                            project,
                            instance,
                            database,
                            spanner_endpoint,
                        );
                        if let Err(err) = connection.run().await {
                            error!("connection error: {}", err);
                        }
                    });
                }
                Err(err) => {
                    error!("failed to accept incoming TCP connection: {}", err);
                }
            }
        }
    }
}
