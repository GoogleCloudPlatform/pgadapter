# SSL Connections

The command line argument `-ssl` can be used to configure SSL support in PGAdapter. The following
three values are supported:
1. `Disabled`: This is the default. SSL connections will be rejected and the client will be asked to
   connect using a plain-text connection.
2. `Enabled`: SSL connections are accepted. Plain-text connections are also accepted.
3. `Required`: Only SSL connections are accepted. Plain-text connections will be rejected.

SSL modes `Enabled` and `Required` require that a private key and a public certificate is added to
the Java keystore, or that PGAdapter is started with `--auto-generate-ssl-key`. The latter will
automatically generate a self-signed private key and public certificate to use for SSL.

## Auto-generating a Self-Signed Certificate


## Adding a Certificate and Private Key to PGAdapter
