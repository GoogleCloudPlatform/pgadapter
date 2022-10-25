FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:slim
RUN apt update && apt -y install postgresql-client
RUN apt -y install default-jre
RUN apt -y install wget
RUN apt -y install python

# ADD ../test-spanner-pgadapter.json /test-spanner-pgadapter.json
ADD https://storage.googleapis.com/pgadapter-jar-releases/pgadapter.tar.gz /pgadapter.tar.gz
RUN tar -xzvf pgadapter.tar.gz

ADD https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz /ycsb-0.17.0.tar.gz
RUN tar xfvz ycsb-0.17.0.tar.gz
RUN mv ycsb-0.17.0 ycsb

ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.0/postgresql-42.5.0.jar /ycsb/jdbc-binding/lib/postgresql-42.5.0.jar
ADD https://repo1.maven.org/maven2/com/kohlschutter/junixsocket/junixsocket-common/2.6.0/junixsocket-common-2.6.0.jar /ycsb/jdbc-binding/lib/junixsocket-common-2.6.0.jar
ADD https://repo1.maven.org/maven2/com/kohlschutter/junixsocket/junixsocket-native-common/2.6.0/junixsocket-native-common-2.6.0.jar /ycsb/jdbc-binding/lib/junixsocket-native-common-2.6.0.jar

# Add startup script.
ADD run.sh /run.sh
RUN chmod +x /run.sh

ENTRYPOINT ["/bin/bash", "/run.sh"]
