# Spring Data JPA Example

This example shows how to use Spring Boot Data JPA with a Cloud Spanner PostgreSQL-dialect database.
The sample application starts PGAdapter as an in-process dependency, and uses the standard
PostgreSQL JDBC driver and Hibernate dialect to connect through the in-process PGAdapter to Cloud
Spanner.

## Running
Modify the `application.properties` file in the [src/main/resources](src/main/resources) directory
to match your Cloud Spanner database. The database must exist and must use the PostgreSQL dialect.
The application will automatically create the required tables when the application is starting.

Run the application from your favorite IDE or execute it from the command line with:

```shell
mvn spring-boot:run
```

See [Troubleshooting](#troubleshooting) if you run into unexpected errors.

## Integration with IntelliJ

It is recommended to [follow these instructions](../../../docs/intellij.md) to add your Cloud
Spanner database as a data source to IntelliJ, if you are using IntelliJ for the development of your
application. This will give you handy features like code-completion and table and column name
validation. It will also give you a PGAdapter instance that is always up and running while IntelliJ
is running.

## Features

The sample application contains entities with mappings for all supported data types in Cloud Spanner.
In addition, the sample application shows how to do the following when using Spring Boot Data JPA
with Cloud Spanner PostgreSQL databases:

| Feature                      | Description                                                                                                                                                                                                                                                                                                                                                                                        |
|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Running PGAdapter in-process | PGAdapter is started in-process along with the main application. This eliminates the requirement of running PGAdapter as a separate process. The [PGAdapter](src/main/java/com/google/cloud/spanner/pgadapter/sample/PGAdapter.java) class shows how to start and stop PGAdapter with your application.                                                                                            |
| Liquibase                    | The sample application uses Liquibase to create the database schema. It is recommended to use a higher-level schema management tool like Liquibase to manage your database schema. This also allows you to use Cloud Spanner-specific features like interleaved tables. The schema definition can be found in [db.changelog-v1.0.sql](src/main/resources/db/changelog/db.changelog-v1.0.sql).      |
| UUID Primary Keys            | All entities in this sample that extend from the [AbstractUuidEntity](src/main/java/com/google/cloud/spanner/pgadapter/sample/model/AbstractUuidEntity.java) class use an automatically generated UUID as the primary key value. This is the recommended type of primary key.                                                                                                                      |
| Sequential Primary Key       | Sequential primary key values can cause hotspots in Cloud Spanner. The [Venue](src/main/java/com/google/cloud/spanner/pgadapter/sample/model/Venue.java) and [Concert](src/main/java/com/google/cloud/spanner/pgadapter/sample/model/Concert.java) entities in this sample application show how you can safely use a sequential auto-generated primary key with Cloud Spanner and Spring Data JPA. |
| Bit-reversed Sequence        | Sequential primary key values can cause hotspots in Cloud Spanner. The [TicketSale](src/main/java/com/google/cloud/spanner/pgadapter/sample/model/TicketSale.java) entity in this sample application shows how you can use a bit-reversed sequence to generate values for the primary key with Cloud Spanner and Spring Data JPA. The generator that is used for the entity supports batching.     |
| Interleaved Tables           | The [Track](src/main/java/com/google/cloud/spanner/pgadapter/sample/model/Track.java) entity is an interleaved table. The table definition is is in [db.changelog-v1.0.sql](src/main/resources/db/changelog/db.changelog-v1.0.sql). The relationship between Track (the child) and Album (the parent) is mapped in JPA as if it was a regular `@ManyToOne` relationship.                           |
| JSONB Fields                 | The [Venue](src/main/java/com/google/cloud/spanner/pgadapter/sample/model/Venue.java) entity contains a JSONB property.                                                                                                                                                                                                                                                                            |
| Read/write Transactions      | Execute read/write transactions on Cloud Spanner. See [SingerService.java](src/main/java/com/google/cloud/spanner/pgadapter/sample/service/SingerService.java) for an example.                                                                                                                                                                                                                     |
| Read-only Transactions       | Execute read-only transactions on Cloud Spanner. It is recommended to use read-only transactions instead of read/write transactions for workloads that only execute read operations. This will improve performance and reduce locking on your database. See [SingerService.java](src/main/java/com/google/cloud/spanner/pgadapter/sample/service/SingerService.java) for an example.               |
| Stale Reads                  | Execute stale reads on Cloud Spanner. Using stale reads can improve performance of your application. See the `staleRead()` method in [SampleApplication.java](src/main/java/com/google/cloud/spanner/pgadapter/sample/SampleApplication.java) for an example.                                                                                                                                      |


### Running PGAdapter in-process

It is recommended to run PGAdapter in-process with your Java application. This simplifies both the
development and deployment process, as you only have one application that needs to be deployed and
started. Running PGAdapter and your application in the same JVM will also give you minimal latency
between your application and PGAdaper.

### UUID Primary Keys

The [AbstractUuidEntity](src/main/java/com/google/cloud/spanner/pgadapter/sample/model/AbstractUuidEntity.java)
is a mapped super class that is used by most of the concrete entities in this sample application. It
defines a primary key of type UUID that is stored as a string. This is the recommended primary key
type when using JPA with Cloud Spanner, as the primary key generation is fully handled in the client.
This reduces the number of round-trips to the database. See also https://cloud.google.com/spanner/docs/schema-and-data-model#choosing_a_primary_key.

### Bit-reversed Sequence

Using a traditional auto-increment primary key with Cloud Spanner is not recommended, because a
monotonically increasing or decreasing primary key value can create a write-hotspot. This will cause
all writes to be sent to one server. See https://cloud.google.com/spanner/docs/schema-design#primary-key-prevent-hotspots
for more background information.

Cloud Spanner therefore supports bit-reversed sequences. These internally work as traditional
sequences, but the value that is returned is bit-reversed before being returned to the user. These
sequences can be used to generate primary keys with JPA / Hibernate.

Note that Hibernate requires sequences to support pooling in order to support efficient batching of
multiple inserts. Pooling normally requires the sequence to support an increment size larger than 1.
Bit-reversed sequences can also support pooling, but require a custom ID generator to be used.
Follow these steps to define an entity that uses a bit-reversed sequence for generating a primary
key that also supports batching:

1. Add the following dependency to your project:

```xml
<!-- Add Spanner Hibernate tools for access to the batch compatible bit-reversed sequence generator. -->
<dependency>
   <groupId>com.google.cloud</groupId>
   <artifactId>google-cloud-spanner-hibernate-tools</artifactId>
   <version>3.1.0</version>
</dependency>
```

2. Add the following annotations to your entity:

```java
  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "ticketSaleId")
  @GenericGenerator(
      // This is the name of the generator, not the name of the sequence. This name must correspond
      // with the name given in the @GeneratedValue above.
      name = "ticketSaleId",
      // Use this custom strategy to ensure the use of a bit-reversed sequence that is compatible
      // with
      // batching multiple inserts. See also
      // https://docs.jboss.org/hibernate/orm/5.4/userguide/html_single/Hibernate_User_Guide.html#batch.
      type = PooledBitReversedSequenceStyleGenerator.class,
      parameters = {
        // Use a separate sequence name for each entity.
        // See resources/db.changelog-v1.1.sql file for the sequence definition in the database.
        @Parameter(name = SequenceStyleGenerator.SEQUENCE_PARAM, value = "ticket_sale_seq"),
        // The increment_size is not actually set on the sequence that is created, but is used to
        // generate a SELECT query that fetches this number of identifiers at once.
        @Parameter(name = SequenceStyleGenerator.INCREMENT_PARAM, value = "200"),
        @Parameter(name = SequenceStyleGenerator.INITIAL_PARAM, value = "50000"),
        // Add any range that should be excluded by the generator if your table already
        // contains existing values that have been generated by other generators.
        @Parameter(
            name = PooledBitReversedSequenceStyleGenerator.EXCLUDE_RANGE_PARAM,
            value = "[1,1000]"),
      })
  public Long id;
```

See [TicketSale.java](src/main/java/com/google/cloud/spanner/pgadapter/sample/model/TicketSale.java)
for a working example.

### Sequential Primary Keys

Using a traditional auto-increment primary key with Cloud Spanner is not recommended, because a
monotonically increasing or decreasing primary key value can create a write-hotspot. This will cause
all writes to be sent to one server. See https://cloud.google.com/spanner/docs/schema-design#primary-key-prevent-hotspots
for more background information.

__It is however possible to use sequentially auto-generated primary keys with JPA / Hibernate, as long
as you follow the recommendations in this section, and as is demonstrated in this sample application.__

1. The sample uses the strategy that is explained here: https://cloud.google.com/spanner/docs/generated-column/how-to#primary-key-generated-column
2. The primary key consists of two columns:
   1. A `shard_id` column that is automatically generated by the database. This column is not mapped to the JPA entities, and your application can safely ignore the fact that it exists.
   2. An `id` column that is a monotonically increasing integer. The value for this column is generated using a [table generator](https://jakarta.ee/specifications/persistence/3.1/apidocs/jakarta.persistence/jakarta/persistence/generationtype#TABLE).
3. Only the `id` column is mapped to the entity. This means that JPA thinks that the primary key only consists of the `id` column, and is therefore also the only value that you need to use in the application.

The [Venue](src/main/java/com/google/cloud/spanner/pgadapter/sample/model/Venue.java) and
[Concert](src/main/java/com/google/cloud/spanner/pgadapter/sample/model/Concert.java) entities
show the configuration for such an identifier.

The table definitions can be found in [db.changelog-v1.0.sql](src/main/resources/db/changelog/db.changelog-v1.0.sql).

This strategy for generating monotonically increasing primary keys can for example be used when
porting an existing application that already uses this type of primary key.

### Interleaved Tables

[Interleaved tables](https://cloud.google.com/spanner/docs/schema-and-data-model#schema-examples)
can be used to define parent-child relationships. This type of relationship cannot be defined
directly in JPA, as it is a type of relationship that is unique to Cloud Spanner. It is however
possible to use interleaved tables with JPA like this:
1. Define the interleaved tables in the [Liquibase change log](src/main/resources/db/changelog/db.changelog-v1.0.sql).
2. The child table must use a composite primary key. See the [Track](src/main/java/com/google/cloud/spanner/pgadapter/sample/model/Track.java) entity for an example for this.
3. Map the parent-child relationship as a normal `@ManyToOne` / `@OneToMany` relationship in the entities. See [Album](src/main/java/com/google/cloud/spanner/pgadapter/sample/model/Album.java) and [Track](src/main/java/com/google/cloud/spanner/pgadapter/sample/model/Track.java)

### JSONB Fields

The [Venue](src/main/java/com/google/cloud/spanner/pgadapter/sample/model/Venue.java) entity has a
JSONB column named `description`. This JSONB field is automatically serialized/deserialized to/from a
plain old Java object.

### Read/Write Transactions

Use the standard JPA `@Transactional` annotation to mark a method as transactional. This will
by default use a read/write transaction. You should try to only use read/write transactions for
methods and workloads that actually write data. Using a read/write transaction for a workload that
only reads data is possible, but will cause unnecessary locks to be taken on Cloud Spanner.

### Read-Only Transactions

Use the `@org.springframework.transaction.annotation.Transactional(readOnly = true)` annotation to
indicate that a method should use a read-only transaction. Using a read-only transaction will ensure
that no locks are taken on Cloud Spanner, while still guaranteeing that all read operations in the
method are consistent.

See the [SingerService](src/main/java/com/google/cloud/spanner/pgadapter/sample/service/SingerService.java)
file for an example of a method that uses a read-only transaction.

### Stale Reads

Cloud Spanner supports both [strong and stale reads](https://cloud.google.com/spanner/docs/timestamp-bounds).
Stale reads can offer better performance than strong reads, as it allows Cloud Spanner to use the
closest available replica without blocking.

Stale reads are not part of the standard JPA interface. It is however possible to execute stale
reads by executing [session management commands](https://cloud.google.com/spanner/docs/jdbc-session-mgmt-commands-pgcompat).
The [StaleReadService](src/main/java/com/google/cloud/spanner/pgadapter/sample/service/StaleReadService.java)
contains an example of a helper method that can be used to execute stale reads.
The [SampleApplication.java](src/main/java/com/google/cloud/spanner/pgadapter/sample/SampleApplication.java)
contains a `staleRead()` method that shows how to use the `StaleReadService`.

## Liquibase
The sample application uses Liquibase to manage the database schema. It is recommended to use
a higher level schema management system like Liquibase to manage your database schema for multiple
reasons:
1. It gives you more control over the schema that is actually created. It also gives you a change log of any changes that are applied to your schema, and allows you to rollback changes that have been made.
2. The Spring Data JPA/Hibernate automatic schema update/creation process does not support specific Cloud Spanner features, like interleaved tables. Using Liquibase allows you to create interleaved tables that can be mapped to your entities.
3. The Spring Data JPA/Hibernate automatic schema update/creation process is not supported for all modification types, as Cloud Spanner does not support the full DDL dialect of PostgreSQL. It also does not support all data types that are supported by PostgreSQL.

The Liquibase change sets are automatically applied when the application is started.

### Rollback Liquibase

You can roll back any schema changes that Liquibase executes. For this, you can run the following
Maven command. The JDBC URL must be replaced by your actual database name. Liquibase is unfortunately
not able to pick up the Spring data source configuration automatically when using the Maven plugin.

Note that Maven will not start PGAdapter for you automatically, so you must manually start PGAdapter
before running this command. The example assumes that PGAdapter is running on port 9030.

```shell
mvn liquibase:rollback \
  "-Dliquibase.rollbackCount=1" \
  "-Dliquibase.url=jdbc:postgresql://localhost:9030/projects%2Fmy-project%2Finstances%2Fmy-instance%2Fdatabases%2Fmy-database?options=-c%20spanner.ddl_transaction_mode=AutocommitExplicitTransaction" \
  "-Dliquibase.changeLogFile=src/main/resources/db/changelog/db.changelog-master.yaml"
```

The `spanner.ddl_transaction_mode=AutocommitExplicitTransaction` addition to the above JDBC connection
URL ensures that PGAdapter will automatically commit any active transaction when it encounters a DDL
statement, and then execute all following DDL statements as a single DDL batch.

## Troubleshooting

### Address already in use

The application starts PGAdapter on port `9432` on your local machine. The following error can occur
when you run the application if another process is already using that port number.

```
Server on port 9432 stopped by exception: java.net.BindException: Address already in use
```

You can change the port number that is used for PGAdapter by changing the value in the
[PGAdapter.java](src/main/java/com/google/cloud/spanner/pgadapter/sample/PGAdapter.java) file.