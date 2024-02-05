# BUILD
FROM maven:3-eclipse-temurin-21 AS build
# Copy over build files to docker image.
COPY LICENSE ./
COPY CONTRIBUTING.md ./
COPY README.md ./
COPY NOTIFICATIONS.md ./
COPY logging.properties ./
COPY src src/
COPY pom.xml ./
COPY license-checks.xml ./
COPY java.header ./
# Download dependencies
RUN mvn dependency:go-offline

# Build from source.
RUN mvn package -Passembly -DskipTests


# Docker image for the YCSB runner.
# We pin the version to 455.0.0, because YCSB requires Python 2.x, and this is the last
# version of the gcloud-cli Docker image that supports Python 2.x.
FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:455.0.0-slim

RUN apt update && apt -y install postgresql-client
RUN apt -y install wget
RUN apt -y install python

ADD https://download.java.net/java/GA/jdk21.0.1/415e3f918a1f4062a0074a2794853d0d/12/GPL/openjdk-21.0.1_linux-x64_bin.tar.gz /openjdk.tar.gz
RUN tar xvfz /openjdk.tar.gz

COPY --from=build target/pgadapter /

ADD https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz /ycsb-0.17.0.tar.gz
RUN tar xfvz ycsb-0.17.0.tar.gz
RUN mv ycsb-0.17.0 ycsb

ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar /ycsb/jdbc-binding/lib/postgresql-42.6.0.jar
ADD https://repo1.maven.org/maven2/com/google/cloud/google-cloud-spanner-jdbc/2.14.2/google-cloud-spanner-jdbc-2.14.2-single-jar-with-dependencies.jar /ycsb/jdbc-binding/lib/google-cloud-spanner-jdbc-2.14.2-single-jar-with-dependencies.jar
ADD https://repo1.maven.org/maven2/com/kohlschutter/junixsocket/junixsocket-common/2.8.1/junixsocket-common-2.8.1.jar /ycsb/jdbc-binding/lib/junixsocket-common-2.8.1.jar
ADD https://repo1.maven.org/maven2/com/kohlschutter/junixsocket/junixsocket-native-common/2.8.1/junixsocket-native-common-2.8.1.jar /ycsb/jdbc-binding/lib/junixsocket-native-common-2.8.1.jar

# Add startup script.
ADD benchmarks/ycsb/run.sh /run.sh
RUN chmod +x /run.sh

ENTRYPOINT ["/bin/bash", "/run.sh"]
