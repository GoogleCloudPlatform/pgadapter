echo "Running tests with $(/usr/lib/postgresql/"${PSQL_VERSION}"/bin/psql -V)"

# add psql version
sed -i "s/testdb_e2e_psql_v../testdb_e2e_psql_v$PSQL_VERSION/" .ci/e2e-expected/backslash-dt.txt
sed -i "s/testdb_e2e_psql_v../testdb_e2e_psql_v$PSQL_VERSION/" .ci/e2e-expected/backslash-dt-param.txt
sed -i "s/testdb_e2e_psql_v../testdb_e2e_psql_v$PSQL_VERSION/" .ci/e2e-expected/backslash-di.txt
sed -i "s/testdb_e2e_psql_v../testdb_e2e_psql_v$PSQL_VERSION/" .ci/e2e-expected/backslash-di-param.txt
sed -i "s/testdb_e2e_psql_v../testdb_e2e_psql_v$PSQL_VERSION/" .ci/e2e-expected/backslash-dn.txt
sed -i "s/testdb_e2e_psql_v../testdb_e2e_psql_v$PSQL_VERSION/" .ci/e2e-expected/backslash-dn-param.txt
# remove --- hyphenated formatting lines for diff comparison
sed -i "s/---[-]*//g" .ci/e2e-expected/backslash-dt.txt
sed -i "s/---[-]*//g" .ci/e2e-expected/backslash-dt-param.txt
sed -i "s/---[-]*//g" .ci/e2e-expected/backslash-di.txt
sed -i "s/---[-]*//g" .ci/e2e-expected/backslash-di-param.txt
sed -i "s/---[-]*//g" .ci/e2e-expected/backslash-dn.txt
sed -i "s/---[-]*//g" .ci/e2e-expected/backslash-dn-param.txt

echo "------Test \"select 1::int8\"------"
echo "select 1::int8;" | /usr/lib/postgresql/"${PSQL_VERSION}"/bin/psql -h localhost -p 4242 -d "${GOOGLE_CLOUD_DATABASE_WITH_VERSION}" > .ci/e2e-result/select-one.txt
diff -i -w -s .ci/e2e-result/select-one.txt .ci/e2e-expected/select-one.txt
RETURN_CODE=$((${RETURN_CODE}||$?))

echo "------Test \"select * from <tablename>\"------"
echo "select * from users;" | /usr/lib/postgresql/"${PSQL_VERSION}"/bin/psql -h localhost -p 4242 -d "${GOOGLE_CLOUD_DATABASE_WITH_VERSION}" > .ci/e2e-result/select-all.txt
diff -i -w -s .ci/e2e-result/select-all.txt .ci/e2e-expected/select-all.txt
RETURN_CODE=$((${RETURN_CODE}||$?))

echo "------Test \"\\d\"------"
echo "\d" | /usr/lib/postgresql/"${PSQL_VERSION}"/bin/psql -h localhost -p 4242 -d "${GOOGLE_CLOUD_DATABASE_WITH_VERSION}" > .ci/e2e-result/backslash-d.txt
diff -i -w -s .ci/e2e-result/backslash-d.txt .ci/e2e-expected/backslash-d.txt
RETURN_CODE=$((${RETURN_CODE}||$?))

echo "------Test \"\\d <tablename>\"------"
echo "\d users" | /usr/lib/postgresql/"${PSQL_VERSION}"/bin/psql -h localhost -p 4242 -d "${GOOGLE_CLOUD_DATABASE_WITH_VERSION}" > .ci/e2e-result/backslash-d-param.txt
diff -i -w -s .ci/e2e-result/backslash-d-param.txt .ci/e2e-expected/backslash-d-param.txt
RETURN_CODE=$((${RETURN_CODE}||$?))

echo "------Test \"\\dt\"------"
echo "\dt" | /usr/lib/postgresql/"${PSQL_VERSION}"/bin/psql -h localhost -p 4242 -d "${GOOGLE_CLOUD_DATABASE_WITH_VERSION}" > .ci/e2e-result/backslash-dt.txt
sed -i "s/---[-]*//g" .ci/e2e-result/backslash-dt.txt
diff -i -w -s .ci/e2e-result/backslash-dt.txt .ci/e2e-expected/backslash-dt.txt
RETURN_CODE=$((${RETURN_CODE}||$?))

echo "------Test \"\\dt <tablename>\------"
echo "\dt users" | /usr/lib/postgresql/"${PSQL_VERSION}"/bin/psql -h localhost -p 4242 -d "${GOOGLE_CLOUD_DATABASE_WITH_VERSION}" > .ci/e2e-result/backslash-dt-param.txt
sed -i "s/---[-]*//g" .ci/e2e-result/backslash-dt-param.txt
diff -i -w -s .ci/e2e-result/backslash-dt-param.txt .ci/e2e-expected/backslash-dt-param.txt
RETURN_CODE=$((${RETURN_CODE}||$?))

echo "------Test \"\\di\"------"
echo "\di" | /usr/lib/postgresql/"${PSQL_VERSION}"/bin/psql -h localhost -p 4242 -d "${GOOGLE_CLOUD_DATABASE_WITH_VERSION}" > .ci/e2e-result/backslash-di.txt
sed -i "s/---[-]*//g" .ci/e2e-result/backslash-di.txt
diff -i -w -s .ci/e2e-result/backslash-di.txt .ci/e2e-expected/backslash-di.txt
RETURN_CODE=$((${RETURN_CODE}||$?))

echo "------Test \"\\di <indexname>\"------"
echo "\di PRIMARY_KEY;" | /usr/lib/postgresql/"${PSQL_VERSION}"/bin/psql -h localhost -p 4242 -d "${GOOGLE_CLOUD_DATABASE_WITH_VERSION}" > .ci/e2e-result/backslash-di-param.txt
sed -i "s/---[-]*//g" .ci/e2e-result/backslash-di-param.txt
diff -i -w -s .ci/e2e-result/backslash-di-param.txt .ci/e2e-expected/backslash-di-param.txt
RETURN_CODE=$((${RETURN_CODE}||$?))

echo "------Test \"\\dn\"------"
echo "\dn" | /usr/lib/postgresql/"${PSQL_VERSION}"/bin/psql -h localhost -p 4242 -d "${GOOGLE_CLOUD_DATABASE_WITH_VERSION}" > .ci/e2e-result/backslash-dn.txt
# ignore effective_timestamp
sed -i -E 's/[0-9]{16,}//' .ci/e2e-result/backslash-dn.txt
sed -i "s/---[-]*//g" .ci/e2e-result/backslash-dn.txt
diff -i -w -s .ci/e2e-result/backslash-dn.txt .ci/e2e-expected/backslash-dn.txt
RETURN_CODE=$((${RETURN_CODE}||$?))

echo "------Test \"\\dn <schemaname>\"------"
echo "\dn public" | /usr/lib/postgresql/"${PSQL_VERSION}"/bin/psql -h localhost -p 4242 -d "${GOOGLE_CLOUD_DATABASE_WITH_VERSION}" > .ci/e2e-result/backslash-dn-param.txt
# ignore effective_timestamp
sed -i -E 's/[0-9]{16,}//' .ci/e2e-result/backslash-dn-param.txt
sed -i "s/---[-]*//g" .ci/e2e-result/backslash-dn-param.txt
diff -i -w -s .ci/e2e-result/backslash-dn-param.txt .ci/e2e-expected/backslash-dn-param.txt
RETURN_CODE=$((${RETURN_CODE}||$?))

echo "------Test \"-c option dml batching\"------"
/usr/lib/postgresql/"${PSQL_VERSION}"/bin/psql -h localhost -p 4242 -d "${GOOGLE_CLOUD_DATABASE_WITH_VERSION}" -c "$(echo $(cat .ci/e2e-batching/dml-batch.txt))" > .ci/e2e-result/dml-batching.txt
diff -i -w -s .ci/e2e-result/dml-batching.txt .ci/e2e-expected/dml-batching.txt
RETURN_CODE=$((${RETURN_CODE}||$?))

echo "------Test \"-c option ddl batching\"------"
/usr/lib/postgresql/"${PSQL_VERSION}"/bin/psql -h localhost -p 4242 -d "${GOOGLE_CLOUD_DATABASE_WITH_VERSION}" -c "$(echo $(cat .ci/e2e-batching/ddl-batch.txt))"
echo "\d" | /usr/lib/postgresql/"${PSQL_VERSION}"/bin/psql -h localhost -p 4242 -d "${GOOGLE_CLOUD_DATABASE_WITH_VERSION}" > .ci/e2e-result/ddl-batching.txt
diff -i -w -s .ci/e2e-result/ddl-batching.txt .ci/e2e-expected/ddl-batching.txt
RETURN_CODE=$((${RETURN_CODE}||$?))

echo "------Test \"-c option ddl-dml mix batching\"------"
/usr/lib/postgresql/"${PSQL_VERSION}"/bin/psql -h localhost -p 4242 -d "${GOOGLE_CLOUD_DATABASE_WITH_VERSION}" -c "$(echo $(cat .ci/e2e-batching/ddl-dml-batch.txt))" &> .ci/e2e-result/ddl-dml-batching.txt
diff -i -w -s .ci/e2e-result/ddl-dml-batching.txt .ci/e2e-expected/ddl-dml-batching.txt
RETURN_CODE=$((${RETURN_CODE}||$?))

echo "------Test \"-c option SELECT DML batching\"------"
/usr/lib/postgresql/"${PSQL_VERSION}"/bin/psql -h localhost -p 4242 -d "${GOOGLE_CLOUD_DATABASE_WITH_VERSION}" -c "$(echo $(cat .ci/e2e-batching/select-dml-batch.txt))" &> .ci/e2e-result/select-dml-batching.txt
diff -i -w -s .ci/e2e-result/select-dml-batching.txt .ci/e2e-expected/select-dml-batching.txt
RETURN_CODE=$((${RETURN_CODE}||$?))

echo "------Test \"-c option SELECT DDL batching\"------"
/usr/lib/postgresql/"${PSQL_VERSION}"/bin/psql -h localhost -p 4242 -d "${GOOGLE_CLOUD_DATABASE_WITH_VERSION}" -c "$(echo $(cat .ci/e2e-batching/select-ddl-batch.txt))" &> .ci/e2e-result/select-ddl-batching.txt
diff -i -w -s .ci/e2e-result/select-ddl-batching.txt .ci/e2e-expected/select-ddl-batching.txt
RETURN_CODE=$((${RETURN_CODE}||$?))

