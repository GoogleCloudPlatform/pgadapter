
# native-image -J-Xmx10g -H:IncludeResources=".*metadata.*json$" -jar pgadapter.jar --no-fallback
export GOOGLE_APPLICATION_CREDENTIALS=
export PORT=5433

git clone git@github.com:GoogleCloudPlatform/pgadapter.git
cd pgadapter
mvn clean package -Passembly -DskipTests
cd target/pgadapter
java -jar pgadapter.jar -p appdev-soda-spanner-staging -i knut-test-ycsb -d ycsb -s ${PORT} &> pgadapter.log &
psql -h /tmp -p ${PORT} -c "CREATE TABLE IF NOT EXISTS usertable (
                 	YCSB_KEY VARCHAR(255) PRIMARY KEY,
                 	FIELD0 TEXT, FIELD1 TEXT,
                 	FIELD2 TEXT, FIELD3 TEXT,
                 	FIELD4 TEXT, FIELD5 TEXT,
                 	FIELD6 TEXT, FIELD7 TEXT,
                 	FIELD8 TEXT, FIELD9 TEXT
                 );
                 CREATE TABLE IF NOT EXISTS run (
                  deployment      varchar,
                  workload        varchar,
                  threads         bigint,
                  batch_size      bigint,
                  operation_count bigint,
                  run_time        bigint,
                  throughput      float,
                  read_avg        float,
                  read_p95        float,
                  read_p99        float,
                  insert_avg      float,
                  insert_p95      float,
                  insert_p99      float,
                  primary key (deployment, workload, threads, batch_size, operation_count)
                );"
pkill -f pgadapter

cd ~
git clone git@github.com:brianfrankcooper/YCSB.git
cd YCSB

mkdir jdbc-binding
cd jdbc-binding
mkdir lib
cd lib
wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.4.0/postgresql-42.4.0.jar
wget https://repo1.maven.org/maven2/com/kohlschutter/junixsocket/junixsocket-common/2.5.0/junixsocket-common-2.5.0.jar
wget https://repo1.maven.org/maven2/com/kohlschutter/junixsocket/junixsocket-native-common/2.5.0/junixsocket-native-common-2.5.0.jar

cd ~/pgadapter
mvn package -Passembly -DskipTests
cd target/pgadapter
java -jar pgadapter.jar -p appdev-soda-spanner-staging -i knut-test-ycsb -d ycsb -s ${PORT} &> pgadapter.log &
sleep 1
psql -h /tmp -p ${PORT} -c "delete from run;"
psql -h /tmp -p ${PORT} -c "set spanner.autocommit_dml_mode='partitioned_non_atomic'; delete from usertable;"
pkill -f pgadapter
cd ~/YCSB

rm uds.properties
cat <<EOT >> uds.properties
db.driver=org.postgresql.Driver
db.url=jdbc:postgresql://localhost/ycsb?socketFactory=org.newsclub.net.unix.AFUNIXSocketFactory\$FactoryArg&socketFactoryArg=/tmp/.s.PGSQL.${PORT}
db.user=admin
db.passwd=admin
EOT

rm tcp.properties
cat <<EOT >> tcp.properties
db.driver=org.postgresql.Driver
db.url=jdbc:postgresql://localhost:${PORT}/ycsb
db.user=admin
db.passwd=admin
EOT

# Run Unix Domain Socket Java 8
cd ../pgadapter
mvn clean package -Passembly -DskipTests
cd target/pgadapter
java -jar pgadapter.jar -p appdev-soda-spanner-staging -i knut-test-ycsb -d ycsb -s ${PORT} &> pgadapter.log &
export DEPLOYMENT=jar-java8-uds
export YCSB_PROPERTY_FILE=uds.properties
export YCSB_COMMAND=run
export WORKLOAD=workloadd
export YCSB_DELETE_AFTER=YES
export BATCH_SIZES="1 5 20 50 100"
cd ../../../YCSB
../pgadapter/.ci/run-ycsb.sh
pkill -f pgadapter

# Run Unix Domain Socket Java 17
cd ../pgadapter
mvn clean package -Passembly -Pnative-image -DskipTests
cd target/pgadapter
java -jar pgadapter.jar -p appdev-soda-spanner-staging -i knut-test-ycsb -d ycsb -s ${PORT} &> pgadapter.log &
export DEPLOYMENT=jar-java17-uds
export YCSB_PROPERTY_FILE=uds.properties
export YCSB_COMMAND=run
export WORKLOAD=workloadd
export YCSB_DELETE_AFTER=YES
export BATCH_SIZES="1 5 20 50 100"
cd ../../../YCSB
../pgadapter/.ci/run-ycsb.sh
pkill -f pgadapter

# Run TCP Java 8
cd ../pgadapter
mvn clean package -Passembly -DskipTests
cd target/pgadapter
java -jar pgadapter.jar -p appdev-soda-spanner-staging -i knut-test-ycsb -d ycsb -s ${PORT} &> pgadapter.log &
export DEPLOYMENT=jar-java8-tcp
export YCSB_PROPERTY_FILE=tcp.properties
export YCSB_COMMAND=run
export WORKLOAD=workloadd
export YCSB_DELETE_AFTER=YES
export BATCH_SIZES="1 5 20 50 100"
cd ../../../YCSB
../pgadapter/.ci/run-ycsb.sh
pkill -f pgadapter

# Run TCP Java 17
cd ../pgadapter
mvn clean package -Passembly -Pnative-image -DskipTests
cd target/pgadapter
java -jar pgadapter.jar -p appdev-soda-spanner-staging -i knut-test-ycsb -d ycsb -s ${PORT} &> pgadapter.log &
export DEPLOYMENT=jar-java17-tcp
export YCSB_PROPERTY_FILE=tcp.properties
export YCSB_COMMAND=run
export WORKLOAD=workloadd
export YCSB_DELETE_AFTER=YES
export BATCH_SIZES="1 5 20 50 100"
cd ../../../YCSB
../pgadapter/.ci/run-ycsb.sh
pkill -f pgadapter

# Run native image Unix domain sockets
# This assumes that the native image has been uploaded manually to the VM.
#cd ../pgadapter
#mvn clean package -Passembly -Pnative-image -DskipTests
#cd target/pgadapter
#native-image -J-Xmx10g -H:IncludeResources=".*metadata.*json$" -jar pgadapter.jar --no-fallback
cd ~/native
./pgadapter -p appdev-soda-spanner-staging -i knut-test-ycsb -d ycsb -s ${PORT} &> pgadapter.log &
export DEPLOYMENT=native-uds
export YCSB_PROPERTY_FILE=uds.properties
export YCSB_COMMAND=run
export WORKLOAD=workloadd
export YCSB_DELETE_AFTER=YES
export BATCH_SIZES="1 5 20 50 100"
cd ~/YCSB
../pgadapter/.ci/run-ycsb.sh
pkill -f pgadapter

# Run native image TCP
cd ~/native
./pgadapter -p appdev-soda-spanner-staging -i knut-test-ycsb -d ycsb -s ${PORT} &> pgadapter.log &
export DEPLOYMENT=native-tcp
export YCSB_PROPERTY_FILE=tcp.properties
export YCSB_COMMAND=run
export WORKLOAD=workloadd
export YCSB_DELETE_AFTER=YES
export BATCH_SIZES="1 5 20 50 100"
cd ~/YCSB
../pgadapter/.ci/run-ycsb.sh
pkill -f pgadapter
