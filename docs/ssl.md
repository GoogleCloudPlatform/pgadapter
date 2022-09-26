# SSL Connections

The command line argument `-ssl` can be used to configure SSL support in PGAdapter. The following
three values are supported:
1. `Disabled`: This is the default. SSL connections will be rejected and the client will be asked to
   connect using a plain-text connection.
2. `Enabled`: SSL connections are accepted. Plain-text connections are also accepted.
3. `Required`: Only SSL connections are accepted. Plain-text connections will be rejected.

SSL modes `Enabled` and `Required` require that a private key and a public certificate is added to
the Java keystore.

## Generate a Self-Signed Private Key and Certificate
PGAdapter can use a self-signed private key and certificate for SSL connections. You can generate
this using the following commands:

1. Generate a private key. Replace the CN and SAN (Subject Alternative Name) with the actual host
   name(s) of your server.

```shell
keytool \
  -genkey \
  -keystore pgadapter.p12 \
  -alias pgadapter \
  -storetype PKCS12 \
  -keypass password \
  -storepass password \
  -keyalg RSA \
  -validity 3650 \
  -dname CN=my.domain.com,OU="My Organization Unit",O="My Organization",L="My Location",C=NO \
  -ext "SAN=DNS:my.domain.com,IP:10.11.12.13,DNS:localhost"
```

2. Export a public certificate from the private key and copy this to the client(s) that will be
   connecting.

```shell
keytool \
  -exportcert \
  -keystore pgadapter.p12 \
  -storepass password \
  -alias pgadapter \
  -rfc \
  -file pgadapter-public-key.pem
```

3. Start PGAdapter with SSL enabled and connect with `psql` with SSL. The SSL keystore and password
   is specified using Java system properties.

```shell
java \
  -Djavax.net.ssl.keyStore=pgadapter.p12 \
  -Djavax.net.ssl.keyStorePassword=password \
  -jar pgadapter.jar \
  -p my-project \
  -i my-instance \
  -s 5432 \
  -ssl enable

psql "\
  sslmode=verify-full \
  sslrootcert=pgadapter-public-key.pem \
  host=localhost \
  port=5432 \
  dbname=my-database"
```

## Generate a Self-Signed Private Key and Certificate Using Docker
This section shows how to generate and use a self-signed private key when running PGAdapter with
Docker.

1. Start an interactive shell session using the PGAdapter Docker image. This will also map the
   current directory to the `/data` directory in the container. Everything that is written to that
   directory will also be written to the current directory on the host machine.

```shell
docker pull gcr.io/cloud-spanner-pg-adapter/pgadapter
docker run -it \
  --entrypoint /bin/bash \
  -v $(pwd):/data \
  -w /data \
  gcr.io/cloud-spanner-pg-adapter/pgadapter
```

2. Generate a private key. Replace the CN and SAN (Subject Alternative Name) with the actual host
   name of your server. Note: This host name *must* also be used by your Docker container (see step
   5).

```shell
keytool \
  -genkey \
  -keystore pgadapter.p12 \
  -alias pgadapter \
  -storetype PKCS12 \
  -keypass password \
  -storepass password \
  -keyalg RSA \
  -validity 3650 \
  -dname CN=pgadapter.mydomain.com,OU="My Organization Unit",O="My Organization",L="My Location",C=NO \
  -ext "SAN=DNS:pgadapter.mydomain.com"
```

3. Export a public certificate from the private key and copy this to the client(s) that will be
   connecting.

```shell
keytool \
  -exportcert \
  -keystore pgadapter.p12 \
  -storepass password \
  -alias pgadapter \
  -rfc \
  -file pgadapter-public-key.pem
```

4. Exit the interactive shell session in the Docker container.

```shell
exit
```

5. Start the PGAdapter Docker container with SSL enabled and the specified host name. Connect with
   `psql` with SSL. The SSL keystore and password is specified using Java system properties.

```shell
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
docker run \
  -p 5432:5432 \
  --hostname pgadapter.mydomain.com \
  -v ${GOOGLE_APPLICATION_CREDENTIALS}:${GOOGLE_APPLICATION_CREDENTIALS}:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS \
  -v $(pwd):/data \
  gcr.io/cloud-spanner-pg-adapter/pgadapter \
  -Djavax.net.ssl.keyStore=/data/pgadapter.p12 \
  -Djavax.net.ssl.keyStorePassword=password \
  -p my-project \
  -i my-instance \
  -ssl enable

psql "\
  sslmode=verify-full \
  sslrootcert=pgadapter-public-key.pem \
  host=localhost \
  port=5432 \
  dbname=my-database"
```

## Adding a Private Key to PGAdapter
You can add a private key to use for SSL connections to PGAdapter by adding it to a Java key store
and setting this as the key store to use when starting PGAdapter.
