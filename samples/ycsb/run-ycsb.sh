
# Make sure you have psql, Java, wget and python installed on the system.

#sudo apt update && apt -y install postgresql-client
#sudo apt -y install default-jre
#sudo apt -y install wget
#sudo apt -y install python

# Change these variables to your database name and service account.
SPANNER_DATABASE="projects/my-project/instances/my-instance/databases/my-database"
CREDENTIALS=/path/to/service-account.json

echo "Downloading YCSB"
wget https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz

echo "Extracting YCSB"
tar xfvz ycsb-0.17.0.tar.gz
mv ycsb-0.17.0 ycsb

echo "Download the PostgreSQL JDBC driver and the additional libraries that are needed to use Unix Domain Sockets"
wget -P ycsb/jdbc-binding/lib/ https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.0/postgresql-42.5.0.jar
wget -P ycsb/jdbc-binding/lib/ https://repo1.maven.org/maven2/com/kohlschutter/junixsocket/junixsocket-common/2.6.2/junixsocket-common-2.6.2.jar
wget -P ycsb/jdbc-binding/lib/ https://repo1.maven.org/maven2/com/kohlschutter/junixsocket/junixsocket-native-common/2.6.2/junixsocket-native-common-2.6.2.jar

echo "Pull and start the PGAdapter Docker container"
docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter
docker run \
  --name pgadapter \
  --rm \
  -d -p 5432:5432 \
  -v /tmp:/tmp \
  -v $CREDENTIALS:/credentials.json:ro \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -c /credentials.json -x
sleep 6


echo "Create the 'usertable' table that is used by YCSB"
psql -h /tmp -d $SPANNER_DATABASE \
     -c "CREATE TABLE IF NOT EXISTS usertable (
           YCSB_KEY VARCHAR(255) PRIMARY KEY,
           FIELD0 TEXT, FIELD1 TEXT,
           FIELD2 TEXT, FIELD3 TEXT,
           FIELD4 TEXT, FIELD5 TEXT,
           FIELD6 TEXT, FIELD7 TEXT,
           FIELD8 TEXT, FIELD9 TEXT
        );"

cd ycsb

# This replaces '/' in the database name with '%2f' to make it URL safe.
# This is necessary when using a fully qualified database name in a JDBC connection URL. 
URL_ENCODED_SPANNER_DATABASE=$(echo $SPANNER_DATABASE | sed -e 's/\//%2f/g')

# Create a properties file that can be used with YCSB.
# This tells YCSB which database it should connect to.
cat <<EOT >> uds.properties
db.driver=org.postgresql.Driver
db.url=jdbc:postgresql://localhost/$URL_ENCODED_SPANNER_DATABASE?socketFactory=org.newsclub.net.unix.AFUNIXSocketFactory\$FactoryArg&socketFactoryArg=/tmp/.s.PGSQL.5432
db.user=
db.passwd=
EOT

# Delete any data currently in the `usertable` table.
# Skip this if you want to reuse existing data.
psql -h /tmp -d $SPANNER_DATABASE \
     -c "set spanner.autocommit_dml_mode='partitioned_non_atomic'; delete from usertable;"

# Load 'workload a' into `usertable`.
# Skip this if you want to reuse existing data.
# See also https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload
echo "Load workload a"
echo ""
./bin/ycsb load jdbc -P workloads/workloada \
  -threads 20 \
  -p recordcount=10000 \
  -p db.batchsize=200 \
  -p jdbc.batchupdateapi=true \
  -P uds.properties \
  -cp "jdbc-binding/lib/*"

# Run 'workload a'.
# Repeat this step without any of the other steps to just repeatedly run a workload.
echo ""
echo "---------------"
echo "Run workload a"
echo ""
./bin/ycsb run jdbc -P workloads/workloada \
  -threads 20 \
  -p hdrhistogram.percentiles=50,95,99 \
  -p operationcount=1000 \
  -p recordcount=10000 \
  -p db.batchsize=20 \
  -p jdbc.batchupdateapi=true \
  -P uds.properties \
  -cp "jdbc-binding/lib/*"


# Stop PGAdapter.
docker stop pgadapter
