mvn clean package -P assembly -DskipTests
cd target/pgadapter
native-image \
  --initialize-at-build-time=com.google.protobuf,com.google.gson,com.google.cloud.spanner.pgadapter.Server \
  -J-Xmx14g \
  -H:IncludeResources=".*" \
  -H:ReflectionConfigurationFiles=../../build/native-image/reflectconfig.json \
  -jar pgadapter.jar \
  --no-fallback
