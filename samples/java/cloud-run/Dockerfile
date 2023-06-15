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

# Use the official maven/Java image to create a build artifact.
FROM maven:3.8-jdk-11 as builder

# Copy local code to the container image.
WORKDIR /app
COPY pom.xml .
COPY src ./src

# Build the application.
RUN mvn package -DskipTests

# Use AdoptOpenJDK for base image.
FROM adoptopenjdk/openjdk11:alpine-jre

# Copy the jar to the production image from the builder stage.
COPY --from=builder /app/target/google-cloud-spanner-pgadapter-samples-cloud-run-*.jar /google-cloud-spanner-pgadapter-samples-cloud-run.jar

# Run the web service on container startup.
CMD ["java", "-Djava.security.egd=file:/dev/./urandom", "-jar", "/google-cloud-spanner-pgadapter-samples-cloud-run.jar"]
