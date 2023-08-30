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

# Start with a Docker image that includes a JRE. This is needed for PGAdapter.
FROM eclipse-temurin:17-jre AS jre

# Use the offical golang image to create a binary.
FROM golang:1.19-buster as builder

# Create and change to the app directory.
WORKDIR /app

# Retrieve application dependencies.
COPY go.* ./
RUN go mod download

# Copy local code to the container image.
COPY . ./

# Build the binary.
RUN go build -v -o server

# Use the official Debian slim image for a lean production container.
FROM debian:buster-slim

# Copy the JRE into the production image.
ENV JAVA_HOME=/opt/java/openjdk
COPY --from=jre $JAVA_HOME $JAVA_HOME
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Copy the app-server binary to the production image from the builder stage.
COPY --from=builder /app/server /app/server

# Add the latest version of PGAdapter to the container.
ADD https://storage.googleapis.com/pgadapter-jar-releases/pgadapter.tar.gz /pgadapter.tar.gz
RUN mkdir /pgadapter
RUN tar -xzf /pgadapter.tar.gz -C /pgadapter

# Copy the startup script that will start both PGAdapter and the app-server.
COPY ./startup.sh /startup.sh
RUN chmod +x /startup.sh

ENTRYPOINT ["/bin/bash", "/startup.sh"]
