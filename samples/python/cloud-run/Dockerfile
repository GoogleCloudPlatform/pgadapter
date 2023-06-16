
# Start with a Docker image that includes a JRE. This is needed for PGAdapter.
FROM eclipse-temurin:17-jre AS jre

# Use the official lightweight Python image.
# https://hub.docker.com/_/python
FROM python:3.11-slim

# Copy the JRE into the Pyton image.
ENV JAVA_HOME=/opt/java/openjdk
COPY --from=jre $JAVA_HOME $JAVA_HOME
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Allow statements and log messages to immediately appear in the logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

# Install production dependencies.
# RUN pip install "psycopg[binary]"
RUN pip install --no-cache-dir -r requirements.txt

# Add the latest version of PGAdapter to the container.
ADD https://storage.googleapis.com/pgadapter-jar-releases/pgadapter.tar.gz /pgadapter.tar.gz
RUN mkdir /pgadapter
RUN tar -xzf /pgadapter.tar.gz -C /pgadapter
# RUN ls -lh /pgadapter/pgadapter.jar

# Copy the startup script that will start both PGAdapter and the web service.
COPY ./startup.sh /startup.sh
RUN chmod +x /startup.sh

# Set an ENTRYPOINT for the container.
# The `startup.sh` file will start both PGAdapter and the web service.
ENTRYPOINT ["/bin/bash", "/startup.sh"]
