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
import { User } from "./entity/User"

function runTest(port: number, test: (dataSource: DataSource) => Promise<void>) {
    const AppDataSource = new DataSource({
        type: "postgres",
        host: "localhost",
        port,
        database: "db",
        entities: [User],
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

require('yargs')
    .demand(2)
    .command(
        'findOneUser <port>',
        'Selects one user by id',
        {},
        opts => runTest(opts.port, testFindOneUser)
    )
    .example('node $0 findOneUser 5432')
    .command(
        'createUser <port>',
        'Creates one user',
        {},
        opts => runTest(opts.port, testCreateUser)
    )
    .example('node $0 createUser 5432')
    .wrap(120)
    .recommendCommands()
    .strict()
    .help().argv;
