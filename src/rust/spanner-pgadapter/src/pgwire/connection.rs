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

use crate::backend::QueryBackend;
use crate::pgwire::extended_query::ExtendedQueryExecutor;
use crate::pgwire::protocol::handshake::{
    BackendKeyData, ParameterStatus, ReadyForQuery, StartupMessage,
};
use crate::pgwire::protocol::{BackendMessage, Encode, FrontendMessage};
use crate::session::Session;
use bytes::BytesMut;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{error, info, warn};

pub struct Connection<B: QueryBackend> {
    pub(crate) stream: TcpStream,
    pub(crate) session: Session<B>,
    pub(crate) project: Option<String>,
    pub(crate) instance: Option<String>,
    pub(crate) database: Option<String>,
    pub(crate) spanner_endpoint: Option<String>,
    pub(crate) message_buffer: Vec<FrontendMessage>,
    pub(crate) extended_protocol: bool,
}

impl<B: QueryBackend> Connection<B> {
    pub fn new(
        stream: TcpStream,
        backend: B,
        project: Option<String>,
        instance: Option<String>,
        database: Option<String>,
        spanner_endpoint: Option<String>,
    ) -> Self {
        Self {
            stream,
            session: Session::new(backend),
            project,
            instance,
            database,
            spanner_endpoint,
            message_buffer: Vec::new(),
            extended_protocol: true,
        }
    }

    pub async fn run(&mut self) -> Result<(), anyhow::Error> {
        info!("handling new client connection");
        self.handle_startup_handshake().await?;
        self.handle_operational_phase().await?;
        Ok(())
    }

    async fn read_startup_packet(&mut self) -> Result<Vec<u8>, anyhow::Error> {
        let mut length_bytes = [0u8; 4];
        if let Err(error) = self.stream.read_exact(&mut length_bytes).await {
            return Err(anyhow::anyhow!(
                "client disconnected during startup: {}",
                error
            ));
        }
        let packet_len = u32::from_be_bytes(length_bytes) as usize;
        if packet_len < 8 || packet_len > 10000 {
            return Err(anyhow::anyhow!(
                "invalid startup packet length: {}",
                packet_len
            ));
        }

        let mut payload = vec![0u8; packet_len - 4];
        self.stream.read_exact(&mut payload).await?;
        Ok(payload)
    }

    async fn handle_startup_handshake(&mut self) -> Result<(), anyhow::Error> {
        loop {
            let payload = match self.read_startup_packet().await {
                Ok(payload) => payload,
                Err(error) => {
                    warn!("{}", error);
                    return Ok(());
                }
            };

            let startup_message = {
                let p_ref = &payload;
                FrontendMessage::decode_startup(p_ref)?
            };
            match startup_message {
                FrontendMessage::SSLRequest => {
                    info!("client requested SSL connection, denying (SSL unsupported)");
                    self.stream.write_all(&[b'N']).await?;
                    self.stream.flush().await?;
                }
                FrontendMessage::GSSENCRequest => {
                    info!("client requested GSS connection, denying (GSS unsupported)");
                    self.stream.write_all(&[b'N']).await?;
                    self.stream.flush().await?;
                }
                FrontendMessage::CancelRequest {
                    process_id,
                    secret_key: _,
                } => {
                    info!(
                        "received query cancel request for process_id: {} (not implemented)",
                        process_id
                    );
                    return Err(anyhow::anyhow!("connection terminated by cancel request"));
                }
                FrontendMessage::Startup(startup) => {
                    self.handle_startup_message(startup).await?;
                    return Ok(());
                }
                other => {
                    return Err(anyhow::anyhow!(
                        "unexpected message during startup phase: {:?}",
                        other
                    ));
                }
            }
        }
    }

    async fn handle_startup_message(
        &mut self,
        startup: StartupMessage,
    ) -> Result<(), anyhow::Error> {
        info!(
            "received client startup message: version {}.{}",
            startup.protocol_version >> 16,
            startup.protocol_version & 0xffff
        );
        for (key, value) in &startup.parameters {
            info!("startup parameter: {} = {}", key, value);
        }

        let dbname = startup
            .parameters
            .get("database")
            .or_else(|| startup.parameters.get("dbname"))
            .map(|s| s.as_ref())
            .or(self.database.as_deref())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "database name not specified in connection request or startup options"
                )
            })?;

        let project = self.project.as_deref().unwrap_or("p");
        let instance = self.instance.as_deref().unwrap_or("i");

        let mut dsn = format!(
            "projects/{}/instances/{}/databases/{}",
            project, instance, dbname
        );
        if let Some(endpoint) = &self.spanner_endpoint {
            dsn = format!("{}/{}", endpoint, dsn);
        }

        self.session.backend.init(dsn).await?;

        self.send_startup_response().await?;
        Ok(())
    }

    async fn send_startup_response(&mut self) -> Result<(), anyhow::Error> {
        let mut response = BytesMut::new();
        BackendMessage::AuthenticationOk.encode(&mut response)?;

        let key_data = BackendKeyData {
            process_id: 1234,
            secret_key: 5678,
        };
        BackendMessage::BackendKeyData(key_data).encode(&mut response)?;

        let server_parameters = [
            ("server_version", "14.1"),
            ("application_name", "PGAdapter"),
            ("is_superuser", "false"),
            ("session_authorization", "PGAdapter"),
            ("integer_datetimes", "on"),
            ("server_encoding", "UTF8"),
            ("client_encoding", "UTF8"),
            ("DateStyle", "ISO,YMD"),
            ("IntervalStyle", "iso_8601"),
            ("standard_conforming_strings", "on"),
            ("TimeZone", "UTC"),
        ];
        for &(name, value) in &server_parameters {
            let parameter_status = ParameterStatus { name, value };
            BackendMessage::ParameterStatus(parameter_status).encode(&mut response)?;
        }

        let ready_for_query = ReadyForQuery { tx_status: b'I' };
        BackendMessage::ReadyForQuery(ready_for_query).encode(&mut response)?;

        self.stream.write_all(&response).await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn handle_operational_phase(&mut self) -> Result<(), anyhow::Error> {
        info!("handshake completed, entering query loop");
        loop {
            let (type_byte, payload) = match self.read_raw_packet().await? {
                Some(packet) => packet,
                None => break,
            };

            let message = {
                let p_ref = &payload;
                FrontendMessage::decode(type_byte, p_ref)?
            };
            info!("received operational message: {:?}", message);

            let should_continue = self.process_operational_message(message).await?;
            if !should_continue {
                break;
            }
        }
        Ok(())
    }

    async fn read_raw_packet(&mut self) -> Result<Option<(u8, Vec<u8>)>, anyhow::Error> {
        let mut type_byte = [0u8; 1];
        if let Err(error) = self.stream.read_exact(&mut type_byte).await {
            if error.kind() == tokio::io::ErrorKind::UnexpectedEof {
                info!("client closed connection");
            } else {
                error!("error reading packet type byte: {}", error);
            }
            return Ok(None);
        }

        let mut length_bytes = [0u8; 4];
        self.stream.read_exact(&mut length_bytes).await?;
        let packet_len = u32::from_be_bytes(length_bytes) as usize;
        if packet_len < 4 {
            return Err(anyhow::anyhow!("invalid packet length: {}", packet_len));
        }

        let mut payload = vec![0u8; packet_len - 4];
        self.stream.read_exact(&mut payload).await?;

        Ok(Some((type_byte[0], payload)))
    }

    async fn process_operational_message(
        &mut self,
        message: FrontendMessage,
    ) -> Result<bool, anyhow::Error> {
        if self.session.in_error_state {
            match message {
                FrontendMessage::Terminate(_) => {
                    info!("client requested connection termination while in error state");
                    return Ok(false);
                }
                FrontendMessage::Sync(_) => {
                    info!("received Sync in error state, clearing error state");
                    self.session.in_error_state = false;
                    ExtendedQueryExecutor::handle_sync(self).await?;
                }
                other => {
                    warn!(
                        "discarding operational message due to error state: {:?}",
                        other
                    );
                }
            }
            return Ok(true);
        }
        ExtendedQueryExecutor::handle_message(self, message).await
    }
}
