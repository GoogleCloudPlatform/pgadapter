# Spring Data JPA Example

This example shows how to use Spring Data JPA with a Cloud Spanner PostgreSQL-dialect database.

### Rollback Liquibase

You can roll back the change that is automatically made by Liquibase by running the following command.
The JDBC URL must be replaced by your actual database name. Liquibase is unfortunately not able to
pick up the Spring data source configuration automatically when using the Maven plugin.

```shell
mvn liquibase:rollback \
  -Dliquibase.rollbackCount=1 \
  -Dliquibase.url=jdbc:postgresql://localhost:9001/projects%2Fspanner-pg-preview-internal%2Finstances%2Feurope-north1%2Fdatabases%2Fspring-boot-hibernate-jpa?options=-c%20spanner.ddl_transaction_mode=AutocommitExplicitTransaction \
  -Dliquibase.changeLogFile=src/main/resources/db/changelog/db.changelog-master.yaml
```
