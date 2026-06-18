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

use std::net::SocketAddr;
use tokio::task::JoinHandle;

pub async fn start<T>(address: &str, service: T) -> anyhow::Result<(String, JoinHandle<()>)>
where
    T: super::google::spanner::v1::spanner_server::Spanner,
{
    let listener = tokio::net::TcpListener::bind(address).await?;
    let addr = listener.local_addr()?;

    let server = tokio::spawn(async {
        let stream = tokio_stream::wrappers::TcpListenerStream::new(listener);

        let _ = tonic::transport::Server::builder()
            .add_service(super::google::spanner::v1::spanner_server::SpannerServer::new(service))
            .serve_with_incoming(stream)
            .await;
    });

    Ok((to_uri(addr), server))
}

pub fn to_uri(addr: SocketAddr) -> String {
    if addr.is_ipv6() {
        format!("http://[{}]:{}", addr.ip(), addr.port())
    } else {
        format!("http://{}:{}", addr.ip(), addr.port())
    }
}
