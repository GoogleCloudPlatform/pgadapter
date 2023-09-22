################################################################################
#                                     BUILD                                    #
################################################################################

FROM maven:3.8.6-eclipse-temurin-17 AS build

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

################################################################################
#                                   RELEASE                                    #
################################################################################

FROM gcr.io/distroless/java17-debian11:nonroot

COPY --from=build --chown=nonroot target/pgadapter /home/pgadapter
COPY --from=build --chown=nonroot LICENSE /home/pgadapter/
COPY --from=build --chown=nonroot CONTRIBUTING.md /home/pgadapter/
COPY --from=build --chown=nonroot README.md /home/pgadapter/
COPY --from=build --chown=nonroot NOTIFICATIONS.md /home/pgadapter/
COPY --from=build --chown=nonroot logging.properties /home/pgadapter/

# Run as nonroot
USER 65532
WORKDIR /home/pgadapter
ENTRYPOINT ["/usr/bin/java", "-jar", "pgadapter.jar"]
