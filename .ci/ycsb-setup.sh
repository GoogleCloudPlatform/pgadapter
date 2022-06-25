
# native-image -J-Xmx10g -H:IncludeResources=".*metadata.*json$" -jar pgadapter.jar --no-fallback


mvn clean package -Passembly -DskipTests
cd target/pgadapter
java -jar pgadapter.jar -p appdev-soda-spanner-staging -i knut-test-ycsb -d ycsb &> pgadapter.log &
psql -h /tmp -c "CREATE TABLE usertable (
                 	YCSB_KEY VARCHAR(255) PRIMARY KEY,
                 	FIELD0 TEXT, FIELD1 TEXT,
                 	FIELD2 TEXT, FIELD3 TEXT,
                 	FIELD4 TEXT, FIELD5 TEXT,
                 	FIELD6 TEXT, FIELD7 TEXT,
                 	FIELD8 TEXT, FIELD9 TEXT
                 );"
psql -h /tmp -c "CREATE TABLE run (
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

cat <<EOT >> uds.properties
db.driver=org.postgresql.Driver
db.url=jdbc:postgresql://localhost/ycsb?socketFactory=org.newsclub.net.unix.AFUNIXSocketFactory\$FactoryArg&socketFactoryArg=/tmp/.s.PGSQL.5432
db.user=admin
db.passwd=admin
EOT

./bin/ycsb run jdbc -P workloads/workloadd -threads 1 -p operationcount=1000 -P uds.properties -cp "jdbc-binding/lib/*"

psql -h /tmp -c "set spanner.autocommit_dml_mode='partitioned_non_atomic'; delete from usertable;"
