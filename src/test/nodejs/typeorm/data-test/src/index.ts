// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import "reflect-metadata"
import { DataSource } from "typeorm"
import {AllTypes} from "./entity/AllTypes";
import { User } from "./entity/User"

function runTest(host: string, port: number, database: string, test: (dataSource: DataSource) => Promise<void>) {
    const AppDataSource = new DataSource({
        type: "postgres",
        host,
        port,
        database,
        entities: [AllTypes, User],
        synchronize: false,
        logging: false,
    })
    AppDataSource.initialize()
    .then(() => {
        test(AppDataSource);
    })
    .catch((error) => console.log(error))
}

async function testFindOneUser(dataSource: DataSource) {
    const repository = dataSource.getRepository(User)
    const user = await repository.findOneBy({id: 1})
    if (user) {
        console.log(`Found user ${user.id} with name ${user.firstName} ${user.lastName}`)
    } else {
        console.log('User with id 1 not found')
    }
}

async function testCreateUser(dataSource: DataSource) {
    const repository = dataSource.getRepository(User)

    const user = new User()
    user.id = 1
    user.firstName = 'Timber'
    user.lastName = 'Saw'
    user.age = 25

    await repository.save(user)

    const foundUser = await repository.findOneBy({firstName: 'Timber', lastName: 'Saw'})
    if (foundUser) {
        console.log(`Found user ${foundUser.id} with name ${foundUser.firstName} ${foundUser.lastName}`)
    } else {
        console.log('User not found')
    }
}

async function testUpdateUser(dataSource: DataSource) {
    const repository = dataSource.getRepository(User)

    const user = await repository.findOneBy({id: 1})
    user.firstName = 'Lumber'
    user.lastName = 'Jack'
    user.age = 45

    await repository.save(user)

    console.log('Updated user 1')
}

async function testDeleteUser(dataSource: DataSource) {
    const repository = dataSource.getRepository(User)

    const user = await repository.findOneBy({id: 1})

    await repository.remove(user)

    console.log('Deleted user 1')
}

async function testFindOneAllTypes(dataSource: DataSource) {
    const repository = dataSource.getRepository(AllTypes)
    const row = await repository.findOneBy({col_bigint: 1})
    if (row) {
        console.log(`Found row ${row.col_bigint}`)
        console.log(row)
    } else {
        console.log('Row with id 1 not found')
    }
}

async function testCreateAllTypes(dataSource: DataSource) {
    const repository = dataSource.getRepository(AllTypes)

    const allTypes = {
        col_bigint: 2,
        col_bool: true,
        col_bytea: Buffer.from(Buffer.from('some random string').toString('base64')),
        col_float8: 0.123456789,
        col_int: 123456789,
        col_numeric: 234.54235,
        // Note: The month argument is zero-based, so 6 in this case means July.
        col_timestamptz: new Date(Date.UTC(2022, 6, 22, 18, 15, 42, 11)),
        col_date: '2022-07-22',
        col_varchar: 'some random string',
    } as AllTypes

    await repository.save(allTypes)
    console.log('Created one record')
}

require('yargs')
    .demand(4)
    .command(
        'findOneUser <host> <port> <database>',
        'Selects one user by id',
        {},
        opts => runTest(opts.host, opts.port, opts.database, testFindOneUser)
    )
    .example('node $0 findOneUser 5432')
    .command(
        'createUser <host> <port> <database>',
        'Creates one user',
        {},
        opts => runTest(opts.host, opts.port, opts.database, testCreateUser)
    )
    .example('node $0 createUser 5432')
    .command(
        'updateUser <host> <port> <database>',
        'Updates one user',
        {},
        opts => runTest(opts.host, opts.port, opts.database, testUpdateUser)
    )
    .example('node $0 updateUser 5432')
    .command(
        'deleteUser <host> <port> <database>',
        'Deletes one user',
        {},
        opts => runTest(opts.host, opts.port, opts.database, testDeleteUser)
    )
    .example('node $0 deleteUser 5432')
    .command(
        'findOneAllTypes <host> <port> <database>',
        'Loads one row with all types',
        {},
        opts => runTest(opts.host, opts.port, opts.database, testFindOneAllTypes)
    )
    .example('node $0 findOneAllTypes 5432')
    .command(
        'createAllTypes <host> <port> <database>',
        'Creates one row with all types',
        {},
        opts => runTest(opts.host, opts.port, opts.database, testCreateAllTypes)
    )
    .example('node $0 createAllTypes 5432')
    .wrap(120)
    .recommendCommands()
    .strict()
    .help().argv;
