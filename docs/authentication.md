# Authentication Options

PGAdapter supports both credentials that are supplied at startup as a command line argument, as well
as credentials that are specified in a connection request. The former will cause PGAdapter to use
the same credentials for all connections. The latter can be used to have different credentials per
connection.

## Credentials at Startup
PGAdapter will use credentials that are given at startup if it is started __without the `-a`__
command line argument. There are two ways that the credentials at startup can be specified:
1. Use the `-c` command line argument to specify a credentials file. This can be a service account
   key file, a user credentials file or an external account credentials file.
2. If the `-c` command line argument is not specified, PGAdapter will look for the default credentials
   of the current environment. See https://cloud.google.com/spanner/docs/getting-started/set-up#set_up_authentication_and_authorization
   for more information on authentication and authorization.

__NOTE__: PGAdapter will always try to use fixed credentials for all connections if the `-a` command
line argument has not been specified. PGAdapter will refuse to start if no credentials can be found.
Start PGAdapter with `-a` if you want the clients that connect to PGAdapter to specify the credentials.

### Examples

```shell
# PGAdapter will use the given credentials for all connections.
java -jar pgadapter.jar -p my-project -i my-instance -c "path/to/credentials.json"

# PGAdapter will use the default credentials of the server environment.
# It will fail to start if no default credentials could be found.
java -jar pgadapter.jar -p my-project -i my-instance
```

## Credentials in Connection Request (Authentication Mode)
PGAdapter will require all connections to supply credentials for each connection if it is started
with the `-a` command line argument. The credentials are sent as a password message. The password
message must contain one of the following:
1. The password field can contain a valid OAuth2 token. The username must be `oauth2`.
2. The password field can contain the JSON payload of a credentials file, for example the contents
   of a service account key file. The username will be ignored in this case.
3. The username field can contain the email address of a service account and the password field can
   contain a private key for that service account. Note: The password must include the
   `-----BEGIN PRIVATE KEY-----` and `-----END PRIVATE KEY-----` header and footer.

### Examples

```shell
# The -a argument instructs PGAdapter to require authentication.
java -jar pgadapter.jar -p my-project -i my-instance -a

# Set the username to 'oauth2' and the password to a valid OAuth2 token.
# Note that PGAdapter will not be able to refresh the OAuth2 token, which means that the connection
# will expire when the token has expired.
PGPASSWORD=$(gcloud auth application-default print-access-token --quiet) \
   psql -U oauth2 -h /tmp -d my-database

# Set the PG password to the contents of a credentials file. This can be both a service account or a
# user account file. The username will be ignored by PGAdapter.
PGPASSWORD=$(cat /path/to/credentials.json) psql -h /tmp -d my-database

# Use the private key of a service account and the client email address for the account.
# This example extracts the private key from a credentials file, but you can also read it from any
# other file or copy-paste it into the password prompt. 
PGPASSWORD=$(grep -o '"private_key": "[^"]*' /path/to/credentials.json | grep -o '[^"]*$') \
   psql -h /tmp -p 5433 -U service-account-name@my-project.iam.gserviceaccount.com -d my-database
```

## Connect with Fully Qualified Database Name
PGAdapter can be started with only the `-a` command line argument. All connection options must then
be given in the connection request from the client. The database name must be given as a fully
qualified database name.

### Example
```shell
# The -a argument instructs PGAdapter to require authentication.
java -jar pgadapter.jar -a

# Set the PG password to the contents of a credentials file. This can be both a service account or a
# user account file. The username will be ignored by PGAdapter.
# Specify the database using a fully qualified name between quotes.
PGPASSWORD=$(cat /path/to/credentials.json) \
  psql -h /tmp -d "projects/my-project/instances/my-instance/databases/my-database"
```

`psql` will by default show the name of the connected database on the prompt. You can shorten this
by changing the default prompt of `psql` to for example show `'~'` when connected to the default
database.

```shell
PGPASSWORD=$(gcloud auth application-default print-access-token --quiet) \
PGDATABASE=projects/my-project/instances/my-instance/databases/my-database \
   psql -U oauth2 -h /tmp \
   -v "PROMPT1=%~%R%x%#" -v "PROMPT2=%~%R%x%#"
```
