# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Start with a Docker image that includes a JRE. This is needed for PGAdapter.
FROM eclipse-temurin:17-jre AS jre

# Use the official lightweight Node.js image.
FROM node:18-slim

# Copy the JRE into the Node.js image.
ENV JAVA_HOME=/opt/java/openjdk
COPY --from=jre $JAVA_HOME $JAVA_HOME
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Create and change to the app directory.
WORKDIR /usr/src/app

# Copy application dependency manifests to the container image.
COPY package*.json ./

# Install production dependencies.
RUN npm install --only=production

# Copy local code to the container image.
COPY . ./

# Add the latest version of PGAdapter to the container.
ADD https://storage.googleapis.com/pgadapter-jar-releases/pgadapter.tar.gz /pgadapter.tar.gz
RUN mkdir /pgadapter
RUN tar -xzf /pgadapter.tar.gz -C /pgadapter

# Copy the startup script that will start both PGAdapter and the web service.
COPY ./startup.sh /startup.sh
RUN chmod +x /startup.sh

ENTRYPOINT ["/bin/bash", "/startup.sh"]
