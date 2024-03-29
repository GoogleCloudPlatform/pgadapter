# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Use the offical golang image to create a binary.
# This is based on Debian and sets the GOPATH to /go.
# https://hub.docker.com/_/golang
FROM golang:1.19-buster as builder

# Create and change to the app directory.
WORKDIR /app

# Retrieve application dependencies.
# This allows the container build to reuse cached dependencies.
# Expecting to copy go.mod and if present go.sum.
COPY go.* ./
RUN go mod download

# Copy local code to the container image.
COPY . ./

# Build the binary.
RUN go build -v -o benchmark

# Create the application image based on the PGAdapter Docker image.
# This will include PGAdapter and all its dependencies in the Docker image.
FROM gcr.io/cloud-spanner-pg-adapter/pgadapter

# Copy the app-server binary to the production image from the builder stage.
COPY --from=builder /app/benchmark /app/benchmark
# Copy the startup script that will first start PGAdapter and then run the benchmark app.
COPY ./startup.sh /startup.sh
RUN chmod +x /startup.sh

# Override the ENTRYPOINT from the base PGAdapter Docker image.
# The `startup.sh` file will start PGAdapter first and then run the benchmark.
ENTRYPOINT ["/bin/bash", "/startup.sh"]
