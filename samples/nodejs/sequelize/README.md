<meta name='keywords' content='pgadapter, sequelize, sequelize.js, spanner, cloud spanner, node, node.js'>

# PGAdapter Spanner and Sequelize

PGAdapter has experimental support for [Sequelize](https://sequelize.org/) with the standard Node.js
`pg` driver. This sample application shows how to connect to PGAdapter with Sequelize, and how to
execute queries and transactions on Cloud Spanner.

The sample uses the Cloud Spanner emulator. You can run the sample on the emulator with this
command:

```shell
npm start
```

PGAdapter and the emulator are started in a Docker test container by the sample application.
Docker is therefore required to be installed on your system to run this sample.
