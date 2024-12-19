<meta name='keywords' content='pgadapter, sequelize, sequelize.js, spanner, cloud spanner, node, node.js'>

# PGAdapter Spanner and Sequelize

PGAdapter can be used with [Sequelize](https://sequelize.org/) with the standard Node.js
`pg` driver. This sample application shows how to connect to PGAdapter with Sequelize, and how to
execute queries and transactions on Cloud Spanner.

The sample uses the Cloud Spanner emulator. You can run the sample on the emulator with this
command:

```shell
npm start
```

PGAdapter and the emulator are started in a Docker test container by the sample application.
Docker is therefore required to be installed on your system to run this sample.

## Performance Considerations

### Query Parameters

[Sequelize](https://sequelize.org/) does not use parameterized queries by default. The following query __will not use
a parameterized query__, which will incur a performance penalty on Spanner.

```typescript
  // DO NOT USE!
  const singers = await Singer.findAll({
    where: {
      lastName: {[Op.like]: pattern },
    },
  });
```

Instead, construct queries with bind parameters like this:

```typescript
  // BEST PRACTICE: USE BIND PARAMETERS
  const singers = await Singer.findAll({
  where: {
    lastName: {[Op.like]: literal('$pattern') },
  },
  bind: {pattern},
});
```

See https://sequelize.org/docs/v6/core-concepts/raw-queries/#bind-parameter for more information on
bind parameters in Sequelize.