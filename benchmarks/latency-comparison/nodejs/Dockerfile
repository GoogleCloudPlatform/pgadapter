# Copyright 2024 Google LLC
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

# Create the application image based on the PGAdapter Docker image.
# This will include PGAdapter and all its dependencies in the Docker image.
FROM gcr.io/cloud-spanner-pg-adapter/pgadapter

# Install Node and npm
RUN apt-get update && \
    apt-get install -y ca-certificates curl gnupg && \
    mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
ARG NODE_MAJOR=22
RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list && \
    apt-get update && \
    apt-get install nodejs -y

COPY package.json /
COPY tsconfig.json /
COPY src /
RUN npm install
RUN npm run build

# Copy the startup script that will first start PGAdapter and then run the benchmark app.
COPY ./startup.sh /startup.sh
RUN chmod +x /startup.sh

# Override the ENTRYPOINT from the base PGAdapter Docker image.
# The `startup.sh` file will start PGAdapter first and then run the benchmark.
ENTRYPOINT ["/bin/bash", "/startup.sh"]
