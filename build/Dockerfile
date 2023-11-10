################################################################################
#                                     BUILD                                    #
################################################################################

FROM maven:3.8.6-eclipse-temurin-17 AS build
ARG TARGETARCH
ARG VERSION=1.5.12

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

RUN wget "https://storage.googleapis.com/cloud-spanner-emulator/releases/$VERSION/cloud-spanner-emulator_linux_$TARGETARCH-$VERSION.tar.gz"
RUN tar xvf "cloud-spanner-emulator_linux_$TARGETARCH-$VERSION.tar.gz"

################################################################################
#                                   RELEASE                                    #
################################################################################

FROM eclipse-temurin:17-jre

COPY --from=build target/pgadapter /home/pgadapter
COPY --from=build LICENSE /home/pgadapter/
COPY --from=build CONTRIBUTING.md /home/pgadapter/
COPY --from=build README.md /home/pgadapter/
COPY --from=build NOTIFICATIONS.md /home/pgadapter/
COPY --from=build logging.properties /home/pgadapter/
COPY --from=build emulator_main /home/

# Add startup script.
ADD build/startup.sh /home/pgadapter/startup.sh
RUN chmod +x /home/pgadapter/startup.sh
RUN chmod u+x /home/emulator_main

ENTRYPOINT ["/bin/bash", "/home/pgadapter/startup.sh"]
