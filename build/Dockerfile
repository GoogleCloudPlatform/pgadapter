################################################################################
#                                     BUILD                                    #
################################################################################

FROM gcr.io/cloud-marketplace/google/ubuntu1804:latest AS build

RUN apt-get update                                                          && \
    apt-get -qq -y install git maven

# Copy over build files to docker image.
COPY src src/
COPY metadata metadata/
COPY pom.xml ./

# Build from source.
RUN mvn clean package

################################################################################
#                                   RELEASE                                    #
################################################################################

FROM gcr.io/cloud-marketplace/google/ubuntu1804:latest

ENV PG_ADAPTER_VERSION=2.0.0

RUN apt-get update                                                          && \
    apt-get -qq -y install default-jre

COPY --from=build target/google-cloud-spanner-pgadapter-${PG_ADAPTER_VERSION}-SNAPSHOT.jar /pgadapter.jar

# Default run command.
ENTRYPOINT java -jar /pgadapter.jar -p ${PROJECT} -i ${INSTANCE}               \
    -d ${DATABASE} -c ${CREDENTIALS} -s ${PORT}
