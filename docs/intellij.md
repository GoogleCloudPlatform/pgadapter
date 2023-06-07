# Connect Cloud Spanner PostgreSQL to JetBrains

Connecting your Cloud Spanner PostgreSQL databases with a JetBrains IDE will give you
automatic code completion and error checking for your database code. You can also run
queries directly from the IDE.

## Connecting

You can use the standard PostgreSQL JDBC driver to connect to Cloud Spanner PostgreSQL
databases using PGAdapter. The PostgreSQL driver is already included in JetBrains products.

### Starting PGAdapter

You need to start PGAdapter before connecting to Cloud Spanner. The quickest way to start it,
is by using the pre-built Docker image. You can instruct IntelliJ or any other JetBrains IDE
to start PGAdapter automatically at startup.

First create a run configuration for PGAdapter:

1. Click on `Run | Edit configurations` and then `Add new run configuration`. Select `Docker image`.
2. Fill in the following:
3. Name: PGAdapter
4. Image ID or name: gcr.io/cloud-spanner-pg-adapter/pgadapter
5. Container name: pgadapter
6. Bind port: 9000:5432 (or choose any other available local port that you want to use)
7. Command: -p <your-project-id> -i <your-instance-id> -c /credentials.json
8. Bind mounts: /local/path/to/credentials.json:/credentials.json:ro (Change the local path to match your own credentials or a local service account file)
9. Run options: --rm (optional)

![PGAdapter Docker Run Configuration](img/pgadapter-docker-container.png?raw=true "PGAdapter Docker Run Configuration")

Then add the Run Configuration for PGAdapter as a startup task to IntelliJ:

1. Click `IntelliJ | Preferences`.
2. Search for `Startup Tasks`.
3. Click `+` and add the PGAdapter Run Configuration.

![PGAdapter Startup Task Configuration](img/pgadapter-startup-task.png?raw=true "PGAdapter Startup Task Configuration")

### Creating a Database Connection

Now you can create a connection to a Cloud Spanner database through PGAdapter using the standard
PostgreSQL driver. Open the Database tool window by clicking on the menu item 
`View | Tool Windows | Databases` and click on the `New | Data Source | PostgreSQL` menu item in the window.

Fill in the following properties in the window:

1. Host: localhost (assuming you are running PGAdapter on your local machine)
2. Port: 9000 (change if you started PGAdapter on a different port than 9000)
3. Authentication: No auth
4. Database: your-database-id
5. You now need to instruct IntelliJ to use the standard JDBC metadata methods to introspect the database. Click on the Advanced tab for this.
6. Select the following option on the Advanced tab of the connection dialog:
7. Click `Expert Options` in the bottom right corner of the dialog.
8. Select the option `Introspect using JDBC metadata`.

![PGAdapter Expert Options](img/pgadapter-expert-options.png?raw=true "PGAdapter Expert Options")

The latter option ensures that IntelliJ will use the standardised JDBC metadata methods for 
introspecting the selected database. Cloud Spanner does not support all pg_catalog tables 
that are available in normal PostgreSQL databases. PGAdapter automatically translates the 
standard pg_catalog queries that are needed for the JDBC metadata methods into queries that 
are supported by Cloud Spanner.

Click OK to create the database connection. You can now let IntelliJ introspect
the database to find its tables and views.

### Execute a Query

Select the database connection that you just created in the Databases view and
click on the `+ | Query Console` menu option to open a new query console. You
can now execute SQL queries against on your Cloud Spanner database.

See the [IntelliJ instructions](https://www.jetbrains.com/help/idea/working-with-database-consoles.html)
for more details on how to work with query consoles.
