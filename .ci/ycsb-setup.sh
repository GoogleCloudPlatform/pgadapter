
# native-image -J-Xmx10g -H:IncludeResources=".*metadata.*json$" -jar pgadapter.jar --no-fallback
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
cd ../..

psql -h /tmp -p ${PORT} -c "delete from run;"
psql -h /tmp -p ${PORT} -c "set spanner.autocommit_dml_mode='partitioned_non_atomic'; delete from usertable;"
pkill -f pgadapter

cat <<EOT >> uds.properties
db.driver=org.postgresql.Driver
db.url=jdbc:postgresql://localhost/ycsb?socketFactory=org.newsclub.net.unix.AFUNIXSocketFactory\$FactoryArg&socketFactoryArg=/tmp/.s.PGSQL.${PORT}
db.user=admin
db.passwd=admin
EOT

cd ../pgadapter
mvn clean package -Passembly -DskipTests
cd target/pgadapter
java -jar pgadapter.jar -p appdev-soda-spanner-staging -i knut-test-ycsb -d ycsb -s ${PORT} &> pgadapter.log &
export DEPLOYMENT=jar-java8
export YCSB_PROPERTY_FILE=uds.properties
export YCSB_COMMAND=run
export WORKLOAD=workloadd

cd ../../../YCSB
../pgadapter/.ci/run-ycsb.sh
