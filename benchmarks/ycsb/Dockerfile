# BUILD
FROM maven:3.8.4-eclipse-temurin-17-alpine AS build
## Copy over build files to docker image.
#COPY LICENSE ./
#COPY CONTRIBUTING.md ./
#COPY README.md ./
#COPY NOTIFICATIONS.md ./
#COPY logging.properties ./
#COPY src src/
#COPY pom.xml ./
#COPY license-checks.xml ./
#COPY java.header ./
## Download dependencies
#RUN mvn dependency:go-offline
#
## Build from source.
#RUN mvn package -Passembly -DskipTests

COPY target/pgadapter target/pgadapter

# Docker image for the YCSB runner.
FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:slim

RUN apt update && apt -y install postgresql-client
RUN apt -y install default-jre
RUN apt -y install wget
RUN apt -y install python

COPY --from=build target/pgadapter /

COPY benchmarks/ycsb/ycsb-0.17.0.tar.gz ./
RUN tar xfvz ycsb-0.17.0.tar.gz
RUN mv ycsb-0.17.0 ycsb

ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.0/postgresql-42.5.0.jar /ycsb/jdbc-binding/lib/postgresql-42.5.0.jar
ADD https://repo1.maven.org/maven2/com/kohlschutter/junixsocket/junixsocket-common/2.6.0/junixsocket-common-2.6.0.jar /ycsb/jdbc-binding/lib/junixsocket-common-2.6.0.jar
ADD https://repo1.maven.org/maven2/com/kohlschutter/junixsocket/junixsocket-native-common/2.6.0/junixsocket-native-common-2.6.0.jar /ycsb/jdbc-binding/lib/junixsocket-native-common-2.6.0.jar

# Add startup script.
ADD benchmarks/ycsb/run.sh /run.sh
RUN chmod +x /run.sh

ENTRYPOINT ["/bin/bash", "/run.sh"]
