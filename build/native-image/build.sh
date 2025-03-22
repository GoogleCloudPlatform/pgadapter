mvn clean
mvn package -P assembly -DskipTests
cd target/pgadapter
native-image \
  --initialize-at-build-time=com.google.protobuf,com.google.gson \
  -J-Xmx14g \
  -H:IncludeResources=".*" \
  -H:ReflectionConfigurationFiles=../../build/native-image/reflectconfig.json \
  -jar pgadapter.jar \
  --no-fallback

  -H:IncludeResources=".*metadata.*json$" \
