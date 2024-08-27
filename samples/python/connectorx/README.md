# connectorx sample

This sample application shows how to connect to Cloud Spanner through PGAdapter using
[connectorx](https://sfu-db.github.io/connector-x/intro.html).
PGAdapter is automatically started in a Docker container by the sample application.

Run the sample on the Spanner Emulator with the following command:

```shell
python connectorx_sample.py
```

Run the sample on a real Spanner database with the following command:

```shell
python connectorx_sample.py \
    -p my-project \
    -i my-instance \
    -i my-database
```
