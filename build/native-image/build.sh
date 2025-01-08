mvn clean
mvn package -P assembly -DskipTests
cd target/pgadapter
native-image \
  --shared \
  --initialize-at-build-time=com.google.protobuf,com.google.gson \
  -J-Xmx14g -H:IncludeResources=".*metadata.*json$" \
  -H:ReflectionConfigurationFiles=../../build/native-image/reflectconfig.json \
  -jar pgadapter.jar \
  --no-fallback
