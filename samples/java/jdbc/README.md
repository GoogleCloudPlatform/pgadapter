# JDBC sample

This sample application shows how to connect to Cloud Spanner through PGAdapter using the standard
`JDBC` PostgreSQL driver. PGAdapter is automatically started in-process together with the sample
application.

The sample application adds the following dependencies:

<!--- {x-version-update-start:google-cloud-spanner-pgadapter:released} -->
```xml
<!-- [START pgadapter_and_jdbc_dependency] -->
<dependency>
  <groupId>org.postgresql</groupId>
  <artifactId>postgresql</artifactId>
  <version>42.6.0</version>
</dependency>
<dependency>
  <groupId>com.google.cloud</groupId>
  <artifactId>google-cloud-spanner-pgadapter</artifactId>
  <version>0.23.0</version>
</dependency>
<!-- [END pgadapter_and_jdbc_dependency] -->
```
<!--- {x-version-update-end} -->

Run the sample with the following command:

```shell
mvn exec:java \
  -Dexec.args=" \
    -p my-project \
    -i my-instance \
    -d my-database"
```
