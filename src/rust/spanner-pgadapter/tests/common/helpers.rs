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

#![allow(dead_code)]

use spanner_pgadapter::backend::mock::MockBackend;
use spanner_pgadapter::pgwire::Connection;
use tokio::net::TcpListener;

/// Global test initialization helper. Registers tracing subscriber and sets a panic hook
/// that exits the process immediately on any background panic to avoid tests hanging.
pub fn init_test() {
    let _ = tracing_subscriber::fmt::try_init();

    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let default_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            default_hook(info);
            std::process::exit(101);
        }));
    });
}

/// Starts a mock PGAdapter test server on an ephemeral loopback port and returns the port number.
pub async fn start_test_server() -> u16 {
    init_test();
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind to ephemeral port");
    let port = listener
        .local_addr()
        .expect("failed to get local addr")
        .port();

    tokio::spawn(async move {
        loop {
            if let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut connection =
                        Connection::new(stream, MockBackend::new(), None, None, None, None);
                    if let Err(error) = connection.run().await {
                        eprintln!("test server connection error: {}", error);
                    }
                });
            }
        }
    });

    port
}

/// Helper to parse incoming client TcpStream frames, returning the transaction status from the first encountered ReadyForQuery indicator.
pub async fn read_until_ready(stream: &mut tokio::net::TcpStream) -> u8 {
    use tokio::io::AsyncReadExt;

    let mut buf = vec![];
    let mut temp_buf = [0u8; 1024];
    loop {
        let mut pos = 0;
        while pos < buf.len() {
            let type_byte = buf[pos];
            if pos + 5 > buf.len() {
                break;
            }
            let len = u32::from_be_bytes([buf[pos + 1], buf[pos + 2], buf[pos + 3], buf[pos + 4]])
                as usize;
            if pos + 1 + len > buf.len() {
                break;
            }
            if type_byte == b'Z' {
                return buf[pos + 5];
            }
            pos += 1 + len;
        }

        let n = stream.read(&mut temp_buf).await.unwrap();
        if n == 0 {
            panic!("connection closed prematurely");
        }
        buf.extend_from_slice(&temp_buf[..n]);
    }
}
