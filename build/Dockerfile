################################################################################
#                                     BUILD                                    #
################################################################################

FROM gcr.io/cloud-marketplace/google/ubuntu1804:latest AS build

ENV CLOUD_SDK_VERSION=340.0.0

RUN apt-get update                                                          && \
    apt-get -qq -y install git maven python

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

# Install google-cloud-sdk to get gcloud.
RUN mkdir -p /usr/local/gcloud                                              && \
    cd /usr/local/gcloud                                                    && \
    curl -s -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz && \
    tar -xf google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz       && \
    /usr/local/gcloud/google-cloud-sdk/install.sh                           && \
    ln -s /usr/local/gcloud/google-cloud-sdk/bin/gcloud /usr/bin/gcloud     && \
    rm google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz

# Build from source.
RUN mvn clean package -P shade -DskipTests

################################################################################
#                                   RELEASE                                    #
################################################################################

FROM gcr.io/cloud-marketplace/google/ubuntu1804:latest

RUN apt-get update                                                          && \
    apt-get -qq -y install default-jre

COPY --from=build target/pgadapter.jar /home/pgadapter/pgadapter.jar
COPY --from=build LICENSE /home/pgadapter/
COPY --from=build CONTRIBUTING.md /home/pgadapter/
COPY --from=build README.md /home/pgadapter/
COPY --from=build NOTIFICATIONS.md /home/pgadapter/
COPY --from=build logging.properties /home/pgadapter/

# Add startup script.
ADD build/startup.sh /home/pgadapter/startup.sh
RUN chmod +x /home/pgadapter/startup.sh

ENTRYPOINT ["/bin/bash", "/home/pgadapter/startup.sh"]
